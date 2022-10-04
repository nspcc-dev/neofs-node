package blobovnicza

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.etcd.io/bbolt"
)

// Open opens an internal database at the configured path with the configured permissions.
//
// If the database file does not exist, it will be created automatically.
func (b *Blobovnicza) Open() error {
	b.log.Debug("creating directory for BoltDB",
		logger.FieldString("path", b.path),
		logger.FieldBool("ro", b.boltOptions.ReadOnly),
	)

	var err error

	if !b.boltOptions.ReadOnly {
		err = util.MkdirAllX(filepath.Dir(b.path), b.perm)
		if err != nil {
			return err
		}
	}

	b.log.Debug("opening BoltDB",
		logger.FieldString("path", b.path),
		logger.FieldStringer("permissions", b.perm),
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
		logger.FieldUint("object size limit", b.objSizeLimit),
		logger.FieldUint("storage size limit", b.fullSizeLimit),
	)

	if size := b.filled.Load(); size != 0 {
		b.log.Debug("already initialized",
			logger.FieldUint("size", size),
		)

		return nil
	}

	err := b.boltDB.Update(func(tx *bbolt.Tx) error {
		return b.iterateBucketKeys(func(lower, upper uint64, key []byte) (bool, error) {
			// create size range bucket

			rangeStr := stringifyBounds(lower, upper)
			b.log.Debug("creating bucket for size range",
				logger.FieldString("range", rangeStr),
			)

			_, err := tx.CreateBucketIfNotExists(key)
			if err != nil {
				return false, fmt.Errorf("(%T) could not create bucket for bounds %s: %w",
					b, rangeStr, err)
			}

			return false, nil
		})
	})
	if err != nil {
		return err
	}

	info, err := os.Stat(b.path)
	if err != nil {
		return fmt.Errorf("can't determine DB size: %w", err)
	}

	b.filled.Store(uint64(info.Size()))
	return err
}

// Close releases all internal database resources.
func (b *Blobovnicza) Close() error {
	b.log.Debug("closing BoltDB",
		logger.FieldString("path", b.path),
	)

	return b.boltDB.Close()
}
