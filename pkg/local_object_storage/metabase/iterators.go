package meta

import (
	"errors"
	"fmt"
	"strconv"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"go.etcd.io/bbolt"
)

// ExpiredObject is a descriptor of expired object from DB.
type ExpiredObject struct {
	typ object.Type

	addr *object.Address
}

// Type returns type of the expired object.
func (e *ExpiredObject) Type() object.Type {
	return e.typ
}

// Address returns address of the expired object.
func (e *ExpiredObject) Address() *object.Address {
	return e.addr
}

// ExpiredObjectHandler is an ExpiredObject handling function.
type ExpiredObjectHandler func(*ExpiredObject) error

// ErrInterruptIterator is returned by iteration handlers
// as a "break" keyword.
var ErrInterruptIterator = errors.New("iterator is interrupted")

// IterateExpired iterates over all objects in DB which are out of date
// relative to epoch.
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

			expiresAt, err := strconv.ParseUint(string(expKey), 10, 64)
			if err != nil {
				return fmt.Errorf("could not parse expiration epoch: %w", err)
			} else if expiresAt >= epoch {
				return nil
			}

			return bktExpired.ForEach(func(idKey, _ []byte) error {
				id := object.NewID()

				err = id.Parse(string(idKey))
				if err != nil {
					return fmt.Errorf("could not parse ID of expired object: %w", err)
				}

				cnrID := cid.New()

				err = cnrID.Parse(string(cidBytes))
				if err != nil {
					return fmt.Errorf("could not parse container ID of expired bucket: %w", err)
				}

				addr := object.NewAddress()
				addr.SetContainerID(cnrID)
				addr.SetObjectID(id)

				return h(&ExpiredObject{
					typ:  objectType(tx, cnrID, idKey),
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

func objectType(tx *bbolt.Tx, cid *cid.ID, oidBytes []byte) object.Type {
	switch {
	default:
		return object.TypeRegular
	case inBucket(tx, tombstoneBucketName(cid), oidBytes):
		return object.TypeTombstone
	case inBucket(tx, storageGroupBucketName(cid), oidBytes):
		return object.TypeStorageGroup
	}
}

// IterateCoveredByTombstones iterates over all objects in DB which are covered
// by tombstone with string address from tss.
//
// If h returns ErrInterruptIterator, nil returns immediately.
// Returns other errors of h directly.
//
// Does not modify tss.
func (db *DB) IterateCoveredByTombstones(tss map[string]struct{}, h func(*object.Address) error) error {
	return db.boltDB.View(func(tx *bbolt.Tx) error {
		return db.iterateCoveredByTombstones(tx, tss, h)
	})
}

func (db *DB) iterateCoveredByTombstones(tx *bbolt.Tx, tss map[string]struct{}, h func(*object.Address) error) error {
	bktGraveyard := tx.Bucket(graveyardBucketName)
	if bktGraveyard == nil {
		return nil
	}

	err := bktGraveyard.ForEach(func(k, v []byte) error {
		if _, ok := tss[string(v)]; ok {
			addr, err := addressFromKey(k)
			if err != nil {
				return fmt.Errorf("could not parse address of the object under tombstone: %w", err)
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
