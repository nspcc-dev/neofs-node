package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	gio "io"
	"slices"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neo-go/pkg/io"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

var (
	ErrIncorrectRootObject = errors.New("invalid root object")
)

// Put updates metabase indexes for the given object.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if obj is of [objectSDK.TypeLock]
// type and there is an object of [objectSDK.TypeTombstone] type associated with
// the same target.
func (db *DB) Put(obj *objectSDK.Object) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	err := db.boltDB.Batch(func(tx *bbolt.Tx) error {
		return db.put(tx, obj, false, currEpoch)
	})
	if err == nil {
		storagelog.Write(db.log,
			storagelog.AddressField(objectCore.AddressOf(obj)),
			storagelog.OpField("metabase PUT"))
	}

	return err
}

func (db *DB) put(
	tx *bbolt.Tx, obj *objectSDK.Object,
	isParent bool, currEpoch uint64) error {
	if err := objectCore.VerifyHeaderForMetadata(*obj); err != nil {
		return err
	}

	exists, err := db.exists(tx, objectCore.AddressOf(obj), currEpoch)

	switch {
	case exists || errors.Is(err, ierrors.ErrParentObject):
		return nil
	case errors.As(err, &apistatus.ObjectNotFound{}):
		// OK, we're putting here.
	case err != nil:
		return err // return any other errors
	}

	if !isParent {
		var par = obj.Parent()

		if par != nil && !par.GetID().IsZero() { // skip the first object without useful info
			_, err := splitInfoFromObject(obj)
			if err != nil {
				return err
			}

			err = db.put(tx, par, true, currEpoch)
			if err != nil {
				return err
			}
		}

		// update container volume size estimation
		if obj.Type() == objectSDK.TypeRegular {
			err = changeContainerInfo(tx, obj.GetContainerID(), int(obj.PayloadSize()), 1)
			if err != nil {
				return err
			}
		}

		err = db.updateCounter(tx, phy, 1, true)
		if err != nil {
			return fmt.Errorf("could not increase phy object counter: %w", err)
		}

		// it is expected that putting an unavailable object is
		// impossible and should be handled on the higher levels
		err = db.updateCounter(tx, logical, 1, true)
		if err != nil {
			return fmt.Errorf("could not increase logical object counter: %w", err)
		}
	}

	err = handleNonRegularObject(tx, currEpoch, *obj)
	if err != nil {
		return err
	}

	if err := PutMetadataForObject(tx, *obj, !isParent); err != nil {
		return fmt.Errorf("put metadata: %w", err)
	}

	return nil
}

func handleNonRegularObject(tx *bbolt.Tx, currEpoch uint64, obj objectSDK.Object) error {
	cID := obj.GetContainerID()
	oID := obj.GetID()
	metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cID))
	if err != nil {
		return fmt.Errorf("create meta bucket for container: %w", err)
	}
	metaCursor := metaBkt.Cursor()
	typ := obj.Type()
	switch typ {
	case objectSDK.TypeLock, objectSDK.TypeTombstone:
		if target := obj.AssociatedObject(); !target.IsZero() {
			typPrefix := make([]byte, metaIDTypePrefixSize)
			fillIDTypePrefix(typPrefix)
			targetTyp, targetTypErr := fetchTypeForID(metaCursor, typPrefix, target)

			if typ == objectSDK.TypeLock {
				if targetTypErr == nil && targetTyp != objectSDK.TypeRegular {
					return logicerr.Wrap(apistatus.LockNonRegularObject{})
				}

				st := objectStatus(tx, metaCursor, oid.NewAddress(cID, target), currEpoch)
				if st == statusTombstoned {
					return logicerr.Wrap(apistatus.ErrObjectAlreadyRemoved)
				}

				if targetTypErr != nil && !errors.Is(targetTypErr, errObjTypeNotFound) {
					return fmt.Errorf("can't get type for %s object's target %s: %w", typ, target, targetTypErr)
				}
			} else { // TS case
				if targetTypErr == nil {
					if targetTyp == objectSDK.TypeTombstone {
						return fmt.Errorf("%s TS's target is another TS: %s", oID, target)
					}
					if targetTyp == objectSDK.TypeLock {
						return ErrLockObjectRemoval
					}
				}

				if objectLocked(tx, currEpoch, metaCursor, cID, target) {
					return apistatus.ErrObjectLocked
				}

				if targetTypErr != nil && !errors.Is(targetTypErr, errObjTypeNotFound) {
					return fmt.Errorf("can't get type for %s object's target %s: %w", typ, target, targetTypErr)
				}

				garbageObjectsBKT := tx.Bucket(garbageObjectsBucketName)
				garbageKey := slices.Concat(cID[:], target[:])
				err := garbageObjectsBKT.Put(garbageKey, zeroValue)
				if err != nil {
					return fmt.Errorf("put %s object to garbage bucket: %w", target, targetTypErr)
				}
			}
		}
	default:
	}

	return nil
}

// encodeList decodes list of bytes into a single blog for list bucket indexes.
func encodeList(lst [][]byte) ([]byte, error) {
	w := io.NewBufBinWriter()
	w.WriteVarUint(uint64(len(lst)))
	for i := range lst {
		w.WriteVarBytes(lst[i])
	}
	if w.Err != nil {
		return nil, w.Err
	}
	return w.Bytes(), nil
}

// decodeList decodes blob into the list of bytes from list bucket index.
func decodeList(data []byte) (lst [][]byte, err error) {
	if len(data) == 0 {
		return nil, nil
	}

	var offset uint64
	size, n, err := getVarUint(data)
	if err != nil {
		return nil, err
	}

	offset += uint64(n)
	lst = make([][]byte, size, size+1)
	for i := range lst {
		sz, n, err := getVarUint(data[offset:])
		if err != nil {
			return nil, err
		}
		offset += uint64(n)

		next := offset + sz
		if uint64(len(data)) < next {
			return nil, gio.ErrUnexpectedEOF
		}
		lst[i] = data[offset:next]
		offset = next
	}
	return lst, nil
}

func getVarUint(data []byte) (uint64, int, error) {
	if len(data) == 0 {
		return 0, 0, gio.ErrUnexpectedEOF
	}

	switch b := data[0]; b {
	case 0xfd:
		if len(data) < 3 {
			return 0, 1, gio.ErrUnexpectedEOF
		}
		return uint64(binary.LittleEndian.Uint16(data[1:])), 3, nil
	case 0xfe:
		if len(data) < 5 {
			return 0, 1, gio.ErrUnexpectedEOF
		}
		return uint64(binary.LittleEndian.Uint32(data[1:])), 5, nil
	case 0xff:
		if len(data) < 9 {
			return 0, 1, gio.ErrUnexpectedEOF
		}
		return binary.LittleEndian.Uint64(data[1:]), 9, nil
	default:
		return uint64(b), 1, nil
	}
}

// splitInfoFromObject returns split info based on last or linkin object.
// Otherwise, returns nil, nil.
func splitInfoFromObject(obj *objectSDK.Object) (*objectSDK.SplitInfo, error) {
	if obj.Parent() == nil {
		return nil, nil
	}

	info := objectSDK.NewSplitInfo()
	info.SetSplitID(obj.SplitID())

	if firstID, set := obj.FirstID(); set {
		info.SetFirstPart(firstID)
	}

	switch {
	case isLinkObject(obj):
		id := obj.GetID()
		if id.IsZero() {
			return nil, errors.New("missing object ID")
		}

		info.SetLink(id)
	case isLastObject(obj):
		id := obj.GetID()
		if id.IsZero() {
			return nil, errors.New("missing object ID")
		}

		info.SetLastPart(id)
	default:
		return nil, ErrIncorrectRootObject // should never happen
	}

	return info, nil
}

// isLinkObject returns true if
// V1: object contains parent header and list
// of children
// V2: object is LINK typed.
func isLinkObject(obj *objectSDK.Object) bool {
	// V2 split
	if obj.Type() == objectSDK.TypeLink {
		return true
	}

	// V1 split
	return len(obj.Children()) > 0 && obj.Parent() != nil
}

// isLastObject returns true if an object has parent and
// V1: object has children in the object's header
// V2: there is no split ID, object's type is LINK, and it has first part's ID.
func isLastObject(obj *objectSDK.Object) bool {
	par := obj.Parent()
	if par == nil {
		return false
	}

	_, hasFirstObjID := obj.FirstID()

	// V2 split
	if obj.SplitID() == nil && (obj.Type() != objectSDK.TypeLink && hasFirstObjID) {
		return true
	}

	// V1 split
	return len(obj.Children()) == 0
}
