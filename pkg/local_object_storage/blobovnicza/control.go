package blobovnicza

import (
	"errors"
	"fmt"
	"path"

	"github.com/nspcc-dev/neofs-node/pkg/util"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// Open opens an internal database at configured path with configured permissions.
//
// If the database file does not exist then it will be created automatically.
func (b *Blobovnicza) Open() error {
	b.log.Debug("creating directory for BoltDB",
		zap.String("path", b.path),
		zap.Bool("ro", b.boltOptions.ReadOnly),
	)

	var err error

	if !b.boltOptions.ReadOnly {
		err = util.MkdirAllX(path.Dir(b.path), b.perm)
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
// If Blobovnicza is already initialized, then no action is taken.
//
// Should not be called in read-only configuration.
func (b *Blobovnicza) Init() error {
	b.log.Debug("initializing...",
		zap.Uint64("object size limit", b.objSizeLimit),
		zap.Uint64("storage size limit", b.fullSizeLimit),
	)

	return b.boltDB.Update(func(tx *bbolt.Tx) error {
		return b.iterateBucketKeys(func(lower, upper uint64, key []byte) (bool, error) {
			// create size range bucket

			b.log.Debug("creating bucket for size range",
				zap.String("range", stringifyBounds(lower, upper)),
			)

			_, err := tx.CreateBucket(key)
			if errors.Is(err, bbolt.ErrBucketExists) {
				// => "smallest" bucket exists => already initialized => do nothing
				// TODO: consider separate bucket structure allocation step
				//  and state initialization/activation

				b.log.Debug("bucket already exists, initializing state")

				return true, b.syncFullnessCounter(tx)
			}

			if err != nil {
				return false, fmt.Errorf("(%T) could not create bucket for bounds [%d:%d]: %w",
					b, lower, upper, err)
			}
			return false, nil
		})
	})
}

// Close releases all internal database resources.
func (b *Blobovnicza) Close() error {
	b.log.Debug("closing BoltDB",
		zap.String("path", b.path),
	)

	return b.boltDB.Close()
}
