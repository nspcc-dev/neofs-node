package meta

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

var tombstoneBucket = []byte("tombstones")

// Delete marks object as deleted.
func (db *DB) Delete(addr *object.Address) error {
	return db.delete(addr)
}

func objectRemoved(tx *bbolt.Tx, addr []byte) bool {
	tombstoneBucket := tx.Bucket(tombstoneBucket)

	return tombstoneBucket != nil && tombstoneBucket.Get(addr) != nil
}

// DeleteObjects marks list of objects as deleted.
func (db *DB) DeleteObjects(list ...*object.Address) {
	if err := db.delete(list...); err != nil {
		db.log.Error("could not delete object list",
			zap.String("error", err.Error()),
		)
	}
}

func (db *DB) delete(list ...*object.Address) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(tombstoneBucket)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not create tombstone bucket", db)
		}

		for i := range list {
			if err := bucket.Put(addressKey(list[i]), nil); err != nil {
				return errors.Wrapf(err, "(%T) could not put to tombstone bucket", db)
			}
		}

		return nil
	})
}
