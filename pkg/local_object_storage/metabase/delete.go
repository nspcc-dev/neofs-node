package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

var tombstoneBucket = []byte("tombstones")

// Delete marks object as deleted.
func (db *DB) Delete(addr *object.Address) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(tombstoneBucket)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not create tombstone bucket", db)
		}

		if err := bucket.Put(addressKey(addr), nil); err != nil {
			return errors.Wrapf(err, "(%T) could not put to tombstone bucket", db)
		}

		return nil
	})
}

func objectRemoved(tx *bbolt.Tx, addr []byte) bool {
	tombstoneBucket := tx.Bucket(tombstoneBucket)

	return tombstoneBucket != nil && tombstoneBucket.Get(addr) != nil
}
