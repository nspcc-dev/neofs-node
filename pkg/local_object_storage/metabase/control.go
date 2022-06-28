package meta

import (
	"fmt"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// Open boltDB instance for metabase.
func (db *DB) Open(readOnly bool) error {
	err := util.MkdirAllX(filepath.Dir(db.info.Path), db.info.Permission)
	if err != nil {
		return fmt.Errorf("can't create dir %s for metabase: %w", db.info.Path, err)
	}

	db.log.Debug("created directory for Metabase", zap.String("path", db.info.Path))

	if db.boltOptions == nil {
		db.boltOptions = bbolt.DefaultOptions
	}
	db.boltOptions.ReadOnly = readOnly

	db.boltDB, err = bbolt.Open(db.info.Path, db.info.Permission, db.boltOptions)
	if err != nil {
		return fmt.Errorf("can't open boltDB database: %w", err)
	}
	db.boltDB.MaxBatchDelay = db.boltBatchDelay
	db.boltDB.MaxBatchSize = db.boltBatchSize

	db.log.Debug("opened boltDB instance for Metabase")

	db.log.Debug("checking metabase version")
	return db.boltDB.View(func(tx *bbolt.Tx) error {
		// The safest way to check if the metabase is fresh is to check if it has no buckets.
		// However, shard info can be present. So here we check that the number of buckets is
		// at most 1.
		// Another thing to consider is that tests do not persist shard ID, we want to support
		// this case too.
		var n int
		err := tx.ForEach(func([]byte, *bbolt.Bucket) error {
			if n++; n >= 2 { // do not iterate a lot
				return errBreakBucketForEach
			}
			return nil
		})

		if err == errBreakBucketForEach {
			db.initialized = true
			err = nil
		}
		return err
	})
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
		string(garbageBucketName):         {},
	}

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		var err error
		if !reset {
			// Normal open, check version and update if not initialized.
			err := checkVersion(tx, db.initialized)
			if err != nil {
				return err
			}
		}
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

		err = tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if _, ok := mStaticBuckets[string(name)]; !ok {
				return tx.DeleteBucket(name)
			}

			return nil
		})
		if err != nil {
			return err
		}
		return updateVersion(tx, version)
	})
}

// Close closes boltDB instance.
func (db *DB) Close() error {
	return db.boltDB.Close()
}
