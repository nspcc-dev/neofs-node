package meta

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/nspcc-dev/bbolt"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
)

// Get returns partial object header data for specified address (as stored in
// metabase). This header contains all user-defined attributes and mandatory
// header fields that can be used as search filters like size, checksum and
// owner. It does not contain parent header or session token.
//
// "raw" flag controls virtual object processing, when false (default) a
// proper object header is returned, when true only SplitInfo of virtual
// object is returned.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in DB.
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
//
// If raw and the object is a parent of some stored objects, Get returns:
// - [object.SplitInfoError] wrapping [object.SplitInfo] collected from parts if object is split;
// - [iec.ErrPartitionedObject] if object is EC.
func (db *DB) Get(addr oid.Address, raw bool) (*object.Object, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}
	var (
		err       error
		hdr       *object.Object
		currEpoch = db.epochState.CurrentEpoch()
	)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		hdr, err = get(tx, addr, true, raw, currEpoch)

		return err
	})

	return hdr, err
}

// If raw and the object is a parent of some stored objects, get returns:
// - [object.SplitInfoError] wrapping [object.SplitInfo] collected from parts if object is split;
// - [iec.ErrPartitionedObject] if object is EC.
func get(tx *bbolt.Tx, addr oid.Address, checkStatus, raw bool, currEpoch uint64) (*object.Object, error) {
	var (
		cnr        = addr.Container()
		metaBucket = tx.Bucket(metaBucketKey(cnr))
		metaCursor *bbolt.Cursor
	)

	if metaBucket != nil {
		metaCursor = metaBucket.Cursor()
	}

	if checkStatus {
		switch objectStatus(tx, metaCursor, addr, currEpoch) {
		case statusGCMarked:
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		case statusTombstoned:
			return nil, logicerr.Wrap(apistatus.ObjectAlreadyRemoved{})
		case statusExpired:
			return nil, ErrObjectIsExpired
		}
	}

	if metaBucket == nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	var objID = addr.Object()

	if raw {
		err := getParentInfo(metaCursor, cnr, objID)
		if errors.Is(err, ierrors.ErrParentObject) {
			return nil, logicerr.Wrap(err)
		}
		// Otherwise it can be a valid non-split object.
	}

	// Reconstruct header from available data.
	var (
		attrs     []object.Attribute
		obj       = object.New()
		objPrefix = slices.Concat([]byte{metaPrefixIDAttr}, objID[:])
	)
	for ak, _ := metaCursor.Seek(objPrefix); bytes.HasPrefix(ak, objPrefix); ak, _ = metaCursor.Next() {
		attrKey, attrVal, ok := bytes.Cut(ak[len(objPrefix):], objectcore.MetaAttributeDelimiter)
		if !ok {
			return nil, fmt.Errorf("invalid attribute in meta of %s/%s: missing delimiter", cnr, objID)
		}
		// Attribute must non-zero key and value.
		if len(attrKey) == 0 || len(attrVal) == 0 {
			return nil, fmt.Errorf("empty attribute or value in meta of %s/%s", cnr, objID)
		}
		switch string(attrKey) {
		case object.FilterVersion:
			var v version.Version
			err := v.DecodeString(string(attrVal))
			if err != nil {
				return nil, fmt.Errorf("invalid version in meta of %s/%s: %w", cnr, objID, err)
			}
			obj.SetVersion(&v)
		case object.FilterOwnerID:
			var u user.ID
			if len(u) != len(attrVal) {
				return nil, fmt.Errorf("invalid owner in meta of %s/%s: length %d", cnr, objID, len(attrVal))
			}
			copy(u[:], attrVal)
			obj.SetOwner(u)
		case object.FilterType:
			var t object.Type

			if !t.DecodeString(string(attrVal)) {
				return nil, fmt.Errorf("invalid type in meta of %s/%s: garbage value", cnr, objID)
			}
			obj.SetType(t)
		case object.FilterCreationEpoch:
			s, err := strconv.ParseUint(string(attrVal), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid epoch in meta of %s/%s: %w", cnr, objID, err)
			}
			obj.SetCreationEpoch(s)
		case object.FilterPayloadSize:
			s, err := strconv.ParseUint(string(attrVal), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid size in meta of %s/%s: %w", cnr, objID, err)
			}
			obj.SetPayloadSize(s)
		case object.FilterPayloadChecksum:
			if len(attrVal) != sha256.Size {
				return nil, fmt.Errorf("invalid checksum in meta of %s/%s: length %d", cnr, objID, len(attrVal))
			}
			var ch = checksum.NewSHA256([sha256.Size]byte(attrVal))
			obj.SetPayloadChecksum(ch)
		case object.FilterPayloadHomomorphicHash:
			if len(attrVal) != tz.Size {
				return nil, fmt.Errorf("invalid homo checksum in meta of %s/%s: length %d", cnr, objID, len(attrVal))
			}
			var ch = checksum.NewTillichZemor([tz.Size]byte(attrVal))
			obj.SetPayloadHomomorphicHash(ch)
		case object.FilterSplitID:
			id := object.NewSplitIDFromV2(attrVal)
			if id == nil {
				return nil, fmt.Errorf("invalid split ID in meta of %s/%s: garbage value", cnr, objID)
			}
			obj.SetSplitID(id)
		case object.FilterFirstSplitObject:
			id, err := oid.DecodeBytes(attrVal)
			if err != nil {
				return nil, fmt.Errorf("invalid first split ID in meta of %s/%s: %w", cnr, objID, err)
			}
			obj.SetFirstID(id)
		case object.FilterParentID:
			id, err := oid.DecodeBytes(attrVal)
			if err != nil {
				return nil, fmt.Errorf("invalid parent ID in meta of %s/%s: %w", cnr, objID, err)
			}
			obj.SetParentID(id)
		case object.FilterPhysical, object.FilterRoot:
			// Not real attributes, ignored.
		default:
			attrs = append(attrs, object.NewAttribute(string(attrKey), string(attrVal)))
		}
	}
	// Any valid object has an owner.
	if obj.Owner().IsZero() {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	obj.SetAttributes(attrs...)
	obj.SetContainerID(cnr)
	obj.SetID(objID)
	return obj, nil
}

func getParentMetaOwnersPrefix(parentID oid.ID) []byte {
	var parentPrefix = make([]byte, 1+len(object.FilterParentID)+attributeDelimiterLen+len(parentID)+attributeDelimiterLen)
	parentPrefix[0] = metaPrefixAttrIDPlain
	off := 1 + copy(parentPrefix[1:], object.FilterParentID)
	off += copy(parentPrefix[off:], objectcore.MetaAttributeDelimiter)
	copy(parentPrefix[off:], parentID[:])

	return parentPrefix
}

func listContainerObjects(tx *bbolt.Tx, cID cid.ID, objs []oid.Address, limit int) ([]oid.Address, error) {
	var metaBkt = tx.Bucket(metaBucketKey(cID))
	if metaBkt == nil {
		return objs, nil
	}

	var cur = metaBkt.Cursor()
	k, _ := cur.Seek([]byte{metaPrefixID})
	for ; len(k) > 0 && len(objs) < limit && k[0] == metaPrefixID; k, _ = cur.Next() {
		obj, err := oid.DecodeBytes(k[1:])
		if err != nil {
			return objs, fmt.Errorf("garbage prefixID key of length %d for container %s: %w", len(k), cID, err)
		}
		objs = append(objs, oid.NewAddress(cID, obj))
	}

	return objs, nil
}
