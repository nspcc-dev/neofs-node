package blobovnicza

import (
	"fmt"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// Open opens an internal database at the configured path with the configured permissions.
//
// If the database file does not exist, it will be created automatically.
func (b *Blobovnicza) Open() error {
	b.log.Debug("creating directory for BoltDB",
		zap.String("path", b.path),
		zap.Bool("ro", b.boltOptions.ReadOnly),
	)

	var err error

	if !b.boltOptions.ReadOnly {
		err = util.MkdirAllX(filepath.Dir(b.path), b.perm)
		if err != nil {
			return err
		}
	}

	b.log.Debug("opening BoltDB",
		zap.String("path", b.path),
		zap.Stringer("permissions", b.perm),
	)

	b.boltDB, err = bbolt.Open(b.path, b.perm, b.boltOptions)

	return err
}

// Init initializes internal database structure.
//
// If Blobovnicza is already initialized, no action is taken.
//
// Should not be called in read-only configuration.
func (b *Blobovnicza) Init() error {
	b.log.Debug("initializing...",
		zap.Uint64("object size limit", b.objSizeLimit),
		zap.Uint64("storage size limit", b.fullSizeLimit),
	)

	if size := b.filled.Load(); size != 0 {
		b.log.Debug("already initialized", zap.Uint64("size", size))
		return nil
	}

	var size uint64

	err := b.boltDB.Update(func(tx *bbolt.Tx) error {
		return b.iterateBucketKeys(func(lower, upper uint64, key []byte) (bool, error) {
			// create size range bucket

			rangeStr := stringifyBounds(lower, upper)
			b.log.Debug("creating bucket for size range",
				zap.String("range", rangeStr))

			buck, err := tx.CreateBucketIfNotExists(key)
			if err != nil {
				return false, fmt.Errorf("(%T) could not create bucket for bounds %s: %w",
					b, rangeStr, err)
			}

			size += uint64(buck.Stats().KeyN) * (upper + lower) / 2
			return false, nil
		})
	})

	b.filled.Store(size)
	return err
}

// Close releases all internal database resources.
func (b *Blobovnicza) Close() error {
	b.log.Debug("closing BoltDB",
		zap.String("path", b.path),
	)

	return b.boltDB.Close()
}
