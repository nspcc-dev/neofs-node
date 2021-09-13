package meta

import (
	"fmt"
	"path"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// Open boltDB instance for metabase.
func (db *DB) Open() error {
	err := util.MkdirAllX(path.Dir(db.info.Path), db.info.Permission)
	if err != nil {
		return fmt.Errorf("can't create dir %s for metabase: %w", db.info.Path, err)
	}

	db.log.Debug("created directory for Metabase", zap.String("path", db.info.Path))

	db.boltDB, err = bbolt.Open(db.info.Path, db.info.Permission, db.boltOptions)
	if err != nil {
		return fmt.Errorf("can't open boltDB database: %w", err)
	}

	db.log.Debug("opened boltDB instance for Metabase")

	return nil
}

// Init initializes metabase. It creates static (CID-independent) buckets in underlying BoltDB instance.
//
// Does nothing if metabase has already been initialized and filled. To roll back the database to its initial state,
// use Reset.
func (db *DB) Init() error {
	return db.init(false)
}

// Reset resets metabase. Works similar to Init but cleans up all static buckets and
// removes all dynamic (CID-dependent) ones in non-blank BoltDB instances.
func (db *DB) Reset() error {
	return db.init(true)
}

func (db *DB) init(reset bool) error {
	mStaticBuckets := map[string]struct{}{
		string(containerVolumeBucketName): {},
		string(graveyardBucketName):       {},
		string(toMoveItBucketName):        {},
	}

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		for k := range mStaticBuckets {
			b, err := tx.CreateBucketIfNotExists([]byte(k))
			if err != nil {
				return fmt.Errorf("could not create static bucket %s: %w", k, err)
			}

			if reset {
				if err = resetBucket(b); err != nil {
					return fmt.Errorf("could not reset static bucket %s: %w", k, err)
				}
			}
		}

		if !reset {
			return nil
		}

		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if _, ok := mStaticBuckets[string(name)]; !ok {
				return tx.DeleteBucket(name)
			}

			return nil
		})
	})
}

// Close closes boltDB instance.
func (db *DB) Close() error {
	return db.boltDB.Close()
}
