package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/nspcc-dev/bbolt"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const metaIDTypePrefixSize = 1 + objectKeySize + len(object.FilterType) + 1

// ExpiredObject is a descriptor of expired object from DB.
type ExpiredObject struct {
	typ object.Type

	addr oid.Address
}

// Type returns type of the expired object.
func (e *ExpiredObject) Type() object.Type {
	return e.typ
}

// Address returns address of the expired object.
func (e *ExpiredObject) Address() oid.Address {
	return e.addr
}

// ExpiredObjectHandler is an ExpiredObject handling function.
type ExpiredObjectHandler func(*ExpiredObject) error

var errObjTypeNotFound = errors.New("object type not found")

// ErrInterruptIterator is returned by iteration handlers
// as a "break" keyword.
var ErrInterruptIterator = logicerr.New("iterator is interrupted")

// IterateExpired iterates over all objects in DB which are out of date
// relative to epoch. Locked objects are not included (do not confuse
// with objects of type LOCK).
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateExpired(epoch uint64, h ExpiredObjectHandler) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateExpired(tx, epoch, h)
	})
}

func keyToEpochOID(k []byte, expStart []byte) (uint64, oid.ID) {
	if !bytes.HasPrefix(k, expStart) || len(k) < len(expStart)+32+objectKeySize {
		return math.MaxUint64, oid.ID{}
	}
	var (
		f256 = k[len(expStart) : len(expStart)+32]
		oidB = k[len(expStart)+32:]
		id   oid.ID
	)
	if !islices.AllZeros(f256[:len(f256)-8]) { // BE integer, too big.
		return math.MaxUint64, oid.ID{}
	}
	var err = id.Decode(oidB)
	if err != nil {
		return math.MaxUint64, oid.ID{}
	}

	return binary.BigEndian.Uint64(f256[32-8:]), id
}

// metaBucket is metadata bucket (0xff), typPrefix is the key for FilterType
// object ID-Attribute key, id is the ID we're looking for.
func fetchTypeForID(metaCursor *bbolt.Cursor, typPrefix []byte, id oid.ID) (object.Type, error) {
	var typ object.Type

	copy(typPrefix[1:], id[:])
	typKey, _ := metaCursor.Seek(typPrefix)
	if bytes.HasPrefix(typKey, typPrefix) {
		var success = typ.DecodeString(string(typKey[len(typPrefix):]))
		if !success {
			return typ, errors.New("can't parse object type")
		}
		return typ, nil
	}
	return typ, errObjTypeNotFound
}

// fillIDTypePrefix puts metaPrefixIDAttr and FilterType properly into typPrefix
// for subsequent use in fetchTypeForID.
func fillIDTypePrefix(typPrefix []byte) {
	typPrefix[0] = metaPrefixIDAttr
	copy(typPrefix[1+objectKeySize:], object.FilterType)
}

func (db *DB) iterateExpired(tx *bbolt.Tx, curEpoch uint64, h ExpiredObjectHandler) error {
	var (
		expStart  = make([]byte, 1+len(object.AttributeExpirationEpoch)+1+1)
		typPrefix = make([]byte, metaIDTypePrefixSize)
	)

	expStart[0] = metaPrefixAttrIDInt
	copy(expStart[1:], object.AttributeExpirationEpoch)
	expStart[len(expStart)-1] = 1 // Positive integer.

	fillIDTypePrefix(typPrefix)

	err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		if len(name) != bucketKeySize || name[0] != metadataPrefix {
			return nil
		}

		var cnrID cid.ID
		err := cnrID.Decode(name[1:])
		if err != nil {
			db.log.Error("could not parse container ID of metadata bucket", zap.Error(err))
			return nil
		}

		var (
			cur          = b.Cursor()
			curForLocked = b.Cursor()
			expKey, _    = cur.Seek(expStart)
			expEpoch, id = keyToEpochOID(expKey, expStart)
		)
		for ; expEpoch < curEpoch && !id.IsZero(); expEpoch, id = keyToEpochOID(expKey, expStart) {
			// Ignore locked objects.
			//
			// To slightly optimize performance we can check only REGULAR objects
			// (only they can be locked), but it's more reliable.
			if objectLocked(tx, curEpoch, curForLocked, cnrID, id) {
				expKey, _ = cur.Next()
				continue
			}

			var addr oid.Address

			addr.SetContainer(cnrID)
			addr.SetObject(id)

			typ, err := fetchTypeForID(b.Cursor(), typPrefix, id)
			if err != nil {
				db.log.Warn("inconsistent DB in expired iterator",
					zap.Stringer("object", addr), zap.Error(err))
			}

			err = h(&ExpiredObject{
				typ:  typ,
				addr: addr,
			})
			if err != nil {
				return err
			}
			expKey, _ = cur.Next()
		}
		return nil
	})

	if errors.Is(err, ErrInterruptIterator) {
		err = nil
	}

	return err
}

func mkFilterPhysicalPrefix() []byte {
	var prefix = make([]byte, 1+len(object.FilterPhysical)+1+len(binPropMarker)+1)

	prefix[0] = metaPrefixAttrIDPlain
	copy(prefix[1:], object.FilterPhysical)
	copy(prefix[1+len(object.FilterPhysical)+1:], binPropMarker)

	return prefix
}

func iteratePhyObjects(tx *bbolt.Tx, f func(cid.ID, oid.ID) error) error {
	var cID cid.ID
	var oID oid.ID

	return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		rawCID, tablePrefix := parseContainerIDWithPrefix(&cID, name)
		if len(rawCID) == 0 || tablePrefix != metadataPrefix {
			return nil
		}

		var (
			c      = b.Cursor()
			prefix = mkFilterPhysicalPrefix()
		)
		for k, _ := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			if oID.Decode(k[len(prefix):]) == nil {
				err := f(cID, oID)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
