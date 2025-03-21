package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

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
	for i := range len(f256) - 8 {
		if f256[i] != 0 { // BE integer, too big.
			return math.MaxUint64, oid.ID{}
		}
	}
	var err = id.Decode(oidB)
	if err != nil {
		return math.MaxUint64, oid.ID{}
	}

	return binary.BigEndian.Uint64(f256[32-8:]), id
}

func (db *DB) iterateExpired(tx *bbolt.Tx, curEpoch uint64, h ExpiredObjectHandler) error {
	var (
		expStart  = make([]byte, 1+len(object.AttributeExpirationEpoch)+1+1)
		typPrefix = make([]byte, 1+objectKeySize+len(object.FilterType)+1)
	)

	expStart[0] = metaPrefixAttrIDInt
	copy(expStart[1:], object.AttributeExpirationEpoch)
	expStart[len(expStart)-1] = 1 // Positive integer.

	typPrefix[0] = metaPrefixIDAttr
	copy(typPrefix[1+objectKeySize:], object.FilterType)

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

		var cur = b.Cursor()
		expKey, _ := cur.Seek(expStart)
		expEpoch, id := keyToEpochOID(expKey, expStart)

		for ; expEpoch < curEpoch && !id.IsZero(); expEpoch, id = keyToEpochOID(expKey, expStart) {
			// Ignore locked objects.
			//
			// To slightly optimize performance we can check only REGULAR objects
			// (only they can be locked), but it's more reliable.
			if objectLocked(tx, cnrID, id) {
				expKey, _ = cur.Next()
				continue
			}

			copy(typPrefix[1:], id[:])

			var (
				addr   oid.Address
				typCur = b.Cursor()
				typ    object.Type
			)

			addr.SetContainer(cnrID)
			addr.SetObject(id)

			typKey, _ := typCur.Seek(typPrefix)
			if bytes.HasPrefix(typKey, typPrefix) {
				var success = typ.DecodeString(string(typKey[len(typPrefix):]))
				if !success {
					db.log.Warn("can't parse object type", zap.Stringer("object", addr))
					// Keep going, treat as regular.
				}
			} else {
				db.log.Warn("object type not found", zap.Stringer("object", addr))
				// Keep going, treat as regular.
			}

			var err = h(&ExpiredObject{
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

// IterateCoveredByTombstones iterates over all objects in DB which are covered
// by tombstone with string address from tss. Locked objects are not included
// (do not confuse with objects of type LOCK).
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
//
// Does not modify tss.
func (db *DB) IterateCoveredByTombstones(tss map[string]oid.Address, h func(oid.Address) error) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateCoveredByTombstones(tx, tss, h)
	})
}

func (db *DB) iterateCoveredByTombstones(tx *bbolt.Tx, tss map[string]oid.Address, h func(oid.Address) error) error {
	bktGraveyard := tx.Bucket(graveyardBucketName)

	err := bktGraveyard.ForEach(func(k, v []byte) error {
		var addr oid.Address
		if err := decodeAddressFromKey(&addr, v[:addressKeySize]); err != nil {
			return err
		}
		if _, ok := tss[addr.EncodeToString()]; ok {
			var addr oid.Address

			err := decodeAddressFromKey(&addr, k)
			if err != nil {
				return fmt.Errorf("could not parse address of the object under tombstone: %w", err)
			}

			if objectLocked(tx, addr.Container(), addr.Object()) {
				return nil
			}

			return h(addr)
		}

		return nil
	})

	if errors.Is(err, ErrInterruptIterator) {
		err = nil
	}

	return err
}

func iteratePhyObjects(tx *bbolt.Tx, f func(cid.ID, oid.ID) error) error {
	var cID cid.ID
	var oID oid.ID

	return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		b58CID, postfix := parseContainerIDWithPrefix(&cID, name)
		if len(b58CID) == 0 {
			return nil
		}

		switch postfix {
		case primaryPrefix,
			storageGroupPrefix,
			lockersPrefix,
			tombstonePrefix:
		default:
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			if oID.Decode(k) == nil {
				return f(cID, oID)
			}

			return nil
		})
	})
}
