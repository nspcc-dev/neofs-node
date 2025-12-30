package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/nspcc-dev/bbolt"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const metaIDTypePrefixSize = 1 + objectKeySize + len(object.FilterType) + 1

// ExpiredObjectHandler is an expired object handling function.
type ExpiredObjectHandler func(oid.Address, object.Type) error

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

// fetchTypeForIDWBuf fetches object type using the given meta bucket cursor
// and reusable key buffer.
func fetchTypeForIDWBuf(metaCursor *bbolt.Cursor, typPrefix []byte, id oid.ID) (object.Type, error) {
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

// fetchTypeForID is similar to fetchTypeForIDWBuf, but allocates a buffer internally.
func fetchTypeForID(c *bbolt.Cursor, id oid.ID) (object.Type, error) {
	var key = make([]byte, metaIDTypePrefixSize)

	fillIDTypePrefix(key)
	return fetchTypeForIDWBuf(c, key, id)
}

// fillIDTypePrefix puts metaPrefixIDAttr and FilterType properly into typPrefix
// for subsequent use in fetchTypeForID.
func fillIDTypePrefix(typPrefix []byte) {
	typPrefix[0] = metaPrefixIDAttr
	copy(typPrefix[1+objectKeySize:], object.FilterType)
}

func fillIDAttributePrefix(s []byte, id oid.ID, attr string) int {
	s[0] = metaPrefixIDAttr
	copy(s[1:], id[:])
	copy(s[1+oid.Size:], attr)
	copy(s[1+oid.Size+len(attr):], objectcore.MetaAttributeDelimiter)
	return attrIDFixedLen + len(attr)
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
		cnrID, prefix := parseContainerIDWithPrefix(name)
		if cnrID.IsZero() || prefix != metadataPrefix {
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
			if objectLocked(curEpoch, curForLocked, id) {
				expKey, _ = cur.Next()
				continue
			}

			var addr oid.Address

			addr.SetContainer(cnrID)
			addr.SetObject(id)

			typ, err := fetchTypeForIDWBuf(b.Cursor(), typPrefix, id)
			if err != nil {
				db.log.Warn("inconsistent DB in expired iterator",
					zap.Stringer("object", addr), zap.Error(err))
			}

			err = h(addr, typ)
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

func iteratePhyObjects(tx *bbolt.Tx, f func(*bbolt.Cursor, oid.ID) error) error {
	var oID oid.ID

	return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		cID, tablePrefix := parseContainerIDWithPrefix(name)
		if cID.IsZero() || tablePrefix != metadataPrefix {
			return nil
		}

		var (
			c      = b.Cursor()
			prefix = mkFilterPhysicalPrefix()
		)
		for k, _ := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			if oID.Decode(k[len(prefix):]) == nil {
				err := f(b.Cursor(), oID)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
