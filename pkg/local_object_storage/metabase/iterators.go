package meta

import (
	"errors"
	"fmt"
	"strconv"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
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
var ErrInterruptIterator = errors.New("iterator is interrupted")

// IterateExpired iterates over all objects in DB which are out of date
// relative to epoch. Locked objects are not included (do not confuse
// with objects of type LOCK).
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
func (db *DB) IterateExpired(epoch uint64, h ExpiredObjectHandler) error {
	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateExpired(tx, epoch, h)
	})
}

func (db *DB) iterateExpired(tx *bbolt.Tx, epoch uint64, h ExpiredObjectHandler) error {
	err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		cidBytes := cidFromAttributeBucket(name, objectV2.SysAttributeExpEpoch)
		if cidBytes == nil {
			return nil
		}

		return b.ForEach(func(expKey, _ []byte) error {
			bktExpired := b.Bucket(expKey)
			if bktExpired == nil {
				return nil
			}

			expiresAfter, err := strconv.ParseUint(string(expKey), 10, 64)
			if err != nil {
				return fmt.Errorf("could not parse expiration epoch: %w", err)
			} else if expiresAfter >= epoch {
				return nil
			}

			return bktExpired.ForEach(func(idKey, _ []byte) error {
				var id oid.ID

				err = id.DecodeString(string(idKey))
				if err != nil {
					return fmt.Errorf("could not parse ID of expired object: %w", err)
				}

				var cnrID cid.ID

				err = cnrID.DecodeString(string(cidBytes))
				if err != nil {
					return fmt.Errorf("could not parse container ID of expired bucket: %w", err)
				}

				// Ignore locked objects.
				//
				// To slightly optimize performance we can check only REGULAR objects
				// (only they can be locked), but it's more reliable.
				if objectLocked(tx, cnrID, id) {
					return nil
				}

				var addr oid.Address
				addr.SetContainer(cnrID)
				addr.SetObject(id)

				return h(&ExpiredObject{
					typ:  firstIrregularObjectType(tx, cnrID, idKey),
					addr: addr,
				})
			})
		})
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
	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateCoveredByTombstones(tx, tss, h)
	})
}

func (db *DB) iterateCoveredByTombstones(tx *bbolt.Tx, tss map[string]oid.Address, h func(oid.Address) error) error {
	bktGraveyard := tx.Bucket(graveyardBucketName)
	if bktGraveyard == nil {
		return nil
	}

	err := bktGraveyard.ForEach(func(k, v []byte) error {
		if _, ok := tss[string(v)]; ok {
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
	var cid cid.ID
	var oid oid.ID

	return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		b58CID, postfix := parseContainerIDWithPostfix(&cid, name)
		if len(b58CID) == 0 {
			return nil
		}

		switch postfix {
		case "",
			storageGroupPostfix,
			bucketNameSuffixLockers,
			tombstonePostfix:
		default:
			return nil
		}

		return b.ForEach(func(k, v []byte) error {
			if oid.DecodeString(string(k)) == nil {
				return f(cid, oid)
			}

			return nil
		})
	})
}
