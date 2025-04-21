package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	gio "io"

	"github.com/nspcc-dev/neo-go/pkg/io"
	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

type (
	namedBucketItem struct {
		name, key, val []byte
	}
)

var (
	ErrUnknownObjectType        = errors.New("unknown object type")
	ErrIncorrectSplitInfoUpdate = errors.New("updating split info on object without it")
	ErrIncorrectRootObject      = errors.New("invalid root object")
)

// Put saves object header in metabase. Object payload is expected to be cut.
//
// binHeader parameter is optional and allows to provide an already encoded
// object header in [DB] format. If provided, the encoding step is skipped.
// It's the caller's responsibility to ensure that the data matches the object
// structure being processed.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (db *DB) Put(obj *objectSDK.Object, binHeader []byte) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	err := db.boltDB.Batch(func(tx *bbolt.Tx) error {
		return db.put(tx, obj, nil, currEpoch, binHeader)
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
	si *objectSDK.SplitInfo, currEpoch uint64, hdrBin []byte) error {
	if err := objectCore.VerifyHeaderForMetadata(*obj); err != nil {
		return err
	}

	isParent := si != nil

	exists, err := db.exists(tx, objectCore.AddressOf(obj), currEpoch)

	switch {
	case errors.As(err, &splitInfoError):
		exists = true // object exists, however it is virtual
	case errors.Is(err, ErrLackSplitInfo), errors.As(err, &apistatus.ObjectNotFound{}):
		// OK, we're putting here.
	case err != nil:
		return err // return any other errors
	}

	// most right child and split header overlap parent so we have to
	// check if object exists to not overwrite it twice
	if exists {
		// when storage already has last object in split hierarchy and there is
		// a linking object to put (or vice versa), we should update split info
		// with object ids of these objects
		if isParent {
			return updateSplitInfo(tx, objectCore.AddressOf(obj), si)
		}

		return nil
	}

	par := obj.Parent()
	if par != nil && !isParent { // limit depth by two
		if parID := par.GetID(); !parID.IsZero() { // skip the first object without useful info
			parentSI, err := splitInfoFromObject(obj)
			if err != nil {
				return err
			}

			err = db.put(tx, par, parentSI, currEpoch, nil)
			if err != nil {
				return err
			}
		}
	}

	err = putUniqueIndexes(tx, obj, si, hdrBin)
	if err != nil {
		return fmt.Errorf("can't put unique indexes: %w", err)
	}

	err = updateListIndexes(tx, obj, putListIndexItem)
	if err != nil {
		return fmt.Errorf("can't put list indexes: %w", err)
	}

	err = updateFKBTIndexes(tx, obj, putFKBTIndexItem)
	if err != nil {
		return fmt.Errorf("can't put fake bucket tree indexes: %w", err)
	}

	// update container volume size estimation
	if obj.Type() == objectSDK.TypeRegular && !isParent {
		err = changeContainerSize(tx, obj.GetContainerID(), obj.PayloadSize(), true)
		if err != nil {
			return err
		}
	}

	if !isParent {
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

	if err := PutMetadataForObject(tx, *obj, !isParent); err != nil {
		return fmt.Errorf("put metadata: %w", err)
	}

	return nil
}

func putUniqueIndexes(
	tx *bbolt.Tx,
	obj *objectSDK.Object,
	si *objectSDK.SplitInfo,
	hdrBin []byte,
) error {
	isParent := si != nil
	addr := objectCore.AddressOf(obj)
	cnr := addr.Container()
	objKey := objectKey(addr.Object(), make([]byte, objectKeySize))

	bucketName := make([]byte, bucketKeySize)
	// add value to primary unique bucket
	if !isParent {
		switch obj.Type() {
		case objectSDK.TypeRegular:
			bucketName = primaryBucketName(cnr, bucketName)
		case objectSDK.TypeTombstone:
			bucketName = tombstoneBucketName(cnr, bucketName)
		case objectSDK.TypeStorageGroup:
			bucketName = storageGroupBucketName(cnr, bucketName)
		case objectSDK.TypeLock:
			bucketName = bucketNameLockers(cnr, bucketName)
		case objectSDK.TypeLink:
			bucketName = linkObjectsBucketName(cnr, bucketName)
		default:
			return ErrUnknownObjectType
		}

		var err error
		if hdrBin == nil {
			hdrBin = obj.CutPayload().Marshal()
		}

		err = putUniqueIndexItem(tx, namedBucketItem{
			name: bucketName,
			key:  objKey,
			val:  hdrBin,
		})
		if err != nil {
			return err
		}
	}

	// index root object
	if obj.Type() == objectSDK.TypeRegular && !obj.HasParent() {
		var (
			err       error
			splitInfo []byte
		)

		if isParent {
			splitInfo = si.Marshal()
		}

		err = putUniqueIndexItem(tx, namedBucketItem{
			name: rootBucketName(cnr, bucketName),
			key:  objKey,
			val:  splitInfo,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

type updateIndexItemFunc = func(tx *bbolt.Tx, item namedBucketItem) error

func updateListIndexes(tx *bbolt.Tx, obj *objectSDK.Object, f updateIndexItemFunc) error {
	idObj := obj.GetID()
	cnr := obj.GetContainerID()
	objKey := objectKey(idObj, make([]byte, objectKeySize))
	bucketName := make([]byte, bucketKeySize)

	cs, _ := obj.PayloadChecksum()

	// index payload hashes
	err := f(tx, namedBucketItem{
		name: payloadHashBucketName(cnr, bucketName),
		key:  cs.Value(),
		val:  objKey,
	})
	if err != nil {
		return err
	}

	idParent := obj.GetParentID()

	// index parent ids
	if !idParent.IsZero() {
		err := f(tx, namedBucketItem{
			name: parentBucketName(cnr, bucketName),
			key:  objectKey(idParent, make([]byte, objectKeySize)),
			val:  objKey,
		})
		if err != nil {
			return err
		}
	}

	// index split ids
	if obj.SplitID() != nil {
		err := f(tx, namedBucketItem{
			name: splitBucketName(cnr, bucketName),
			key:  obj.SplitID().ToV2(),
			val:  objKey,
		})
		if err != nil {
			return err
		}
	}

	// index first object id
	if firstID, set := obj.FirstID(); set {
		err := f(tx, namedBucketItem{
			name: firstObjectIDBucketName(cnr, bucketName),
			key:  firstID[:],
			val:  objKey,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func updateFKBTIndexes(tx *bbolt.Tx, obj *objectSDK.Object, f updateIndexItemFunc) error {
	id := obj.GetID()
	cnr := obj.GetContainerID()
	objKey := objectKey(id, make([]byte, objectKeySize))

	attrs := obj.Attributes()

	key := make([]byte, bucketKeySize)
	err := f(tx, namedBucketItem{
		name: ownerBucketName(cnr, key),
		key:  []byte(obj.Owner().EncodeToString()),
		val:  objKey,
	})
	if err != nil {
		return err
	}

	// user specified attributes
	for i := range attrs {
		key = attributeBucketName(cnr, attrs[i].Key(), key)
		err := f(tx, namedBucketItem{
			name: key,
			key:  []byte(attrs[i].Value()),
			val:  objKey,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func putUniqueIndexItem(tx *bbolt.Tx, item namedBucketItem) error {
	bkt, err := tx.CreateBucketIfNotExists(item.name)
	if err != nil {
		return fmt.Errorf("can't create index %v: %w", item.name, err)
	}

	return bkt.Put(item.key, item.val)
}

func putFKBTIndexItem(tx *bbolt.Tx, item namedBucketItem) error {
	bkt, err := tx.CreateBucketIfNotExists(item.name)
	if err != nil {
		return fmt.Errorf("can't create index %v: %w", item.name, err)
	}

	fkbtRoot, err := bkt.CreateBucketIfNotExists(item.key)
	if err != nil {
		return fmt.Errorf("can't create fake bucket tree index %v: %w", item.key, err)
	}

	return fkbtRoot.Put(item.val, zeroValue)
}

func putListIndexItem(tx *bbolt.Tx, item namedBucketItem) error {
	bkt, err := tx.CreateBucketIfNotExists(item.name)
	if err != nil {
		return fmt.Errorf("can't create index %v: %w", item.name, err)
	}

	lst, err := decodeList(bkt.Get(item.key))
	if err != nil {
		return fmt.Errorf("can't decode leaf list %v: %w", item.key, err)
	}

	lst = append(lst, item.val)

	encodedLst, err := encodeList(lst)
	if err != nil {
		return fmt.Errorf("can't encode leaf list %v: %w", item.key, err)
	}

	return bkt.Put(item.key, encodedLst)
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

// updateSplitInfo for existing objects if storage filled with extra information
// about last object in split hierarchy or linking object.
func updateSplitInfo(tx *bbolt.Tx, addr oid.Address, from *objectSDK.SplitInfo) error {
	key := make([]byte, bucketKeySize)
	bkt := tx.Bucket(rootBucketName(addr.Container(), key))
	if bkt == nil {
		// if object doesn't exists and we want to update split info on it
		// then ignore, this should never happen
		return ErrIncorrectSplitInfoUpdate
	}

	objectKey := objectKey(addr.Object(), key)

	rawSplitInfo := bkt.Get(objectKey)
	if len(rawSplitInfo) == 0 {
		return ErrIncorrectSplitInfoUpdate
	}

	to := objectSDK.NewSplitInfo()

	err := to.Unmarshal(rawSplitInfo)
	if err != nil {
		return fmt.Errorf("can't unmarshal split info from root index: %w", err)
	}

	result := util.MergeSplitInfo(from, to)

	return bkt.Put(objectKey, result.Marshal())
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
