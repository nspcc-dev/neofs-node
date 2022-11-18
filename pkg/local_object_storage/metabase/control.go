package meta

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// ErrDegradedMode is returned when metabase is in a degraded mode.
var ErrDegradedMode = logicerr.New("metabase is in a degraded mode")

// ErrReadOnlyMode is returned when metabase is in a read-only mode.
var ErrReadOnlyMode = logicerr.New("metabase is in a read-only mode")

// Open boltDB instance for metabase.
func (db *DB) Open(readOnly bool) error {
	err := util.MkdirAllX(filepath.Dir(db.info.Path), db.info.Permission)
	if err != nil {
		return fmt.Errorf("can't create dir %s for metabase: %w", db.info.Path, err)
	}

	db.log.Debug("created directory for Metabase", zap.String("path", db.info.Path))

	if db.boltOptions == nil {
		opts := *bbolt.DefaultOptions
		db.boltOptions = &opts
	}
	db.boltOptions.ReadOnly = readOnly

	return db.openBolt()
}

func (db *DB) openBolt() error {
	var err error

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
// Returns ErrOutdatedVersion if a database at the provided path is outdated.
//
// Does nothing if metabase has already been initialized and filled. To roll back the database to its initial state,
// use Reset.
func (db *DB) Init() error {
	return db.init(false)
}

// Reset resets metabase. Works similar to Init but cleans up all static buckets and
// removes all dynamic (CID-dependent) ones in non-blank BoltDB instances.
func (db *DB) Reset() error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	}

	return db.init(true)
}

func (db *DB) init(reset bool) error {
	if db.mode.NoMetabase() || db.mode.ReadOnly() {
		return nil
	}

	mStaticBuckets := map[string]struct{}{
		string(containerVolumeBucketName): {},
		string(graveyardBucketName):       {},
		string(toMoveItBucketName):        {},
		string(garbageBucketName):         {},
		string(shardInfoBucket):           {},
		string(bucketNameLocked):          {},
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
			name := []byte(k)
			if reset {
				err := tx.DeleteBucket(name)
				if err != nil && !errors.Is(err, bbolt.ErrBucketNotFound) {
					return fmt.Errorf("could not delete static bucket %s: %w", k, err)
				}
			}

			_, err := tx.CreateBucketIfNotExists(name)
			if err != nil {
				return fmt.Errorf("could not create static bucket %s: %w", k, err)
			}
		}

		if !reset {
			err = syncCounter(tx, false)
			if err != nil {
				return fmt.Errorf("could not sync object counter: %w", err)
			}

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

// SyncCounters forces to synchronize the object counters.
func (db *DB) SyncCounters() error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		return syncCounter(tx, true)
	})
}

// Close closes boltDB instance.
func (db *DB) Close() error {
	if db.boltDB != nil {
		return db.boltDB.Close()
	}
	return nil
}

// Reload reloads part of the configuration.
// It returns true iff database was reopened.
// If a config option is invalid, it logs an error and returns nil.
// If there was a problem with applying new configuration, an error is returned.
//
// If a metabase was couldn't be reopened because of an error, ErrDegradedMode is returned.
func (db *DB) Reload(opts ...Option) (bool, error) {
	var c cfg
	for i := range opts {
		opts[i](&c)
	}

	db.modeMtx.Lock()
	defer db.modeMtx.Unlock()

	if db.mode.NoMetabase() || c.info.Path != "" && filepath.Clean(db.info.Path) != filepath.Clean(c.info.Path) {
		if err := db.Close(); err != nil {
			return false, err
		}

		db.mode = mode.Degraded
		db.info.Path = c.info.Path
		if err := db.openBolt(); err != nil {
			return false, fmt.Errorf("%w: %v", ErrDegradedMode, err)
		}

		db.mode = mode.ReadWrite
		return true, nil
	}

	return false, nil
}
