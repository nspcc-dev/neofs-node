package meta

import (
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/bbolt"
)

var lastResyncEpochKey = []byte("last_resync_epoch")

// ReadLastResyncEpoch reads from db last epoch when metabase was resynchronized.
// If id is missing, returns 0, nil.
func (db *DB) ReadLastResyncEpoch() (epoch uint64, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return 0, ErrDegradedMode
	}

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		epoch = getLastResyncEpoch(tx)
		return nil
	})

	return
}

// updateLastResyncEpoch updates db value of last epoch when metabase was resynchronized.
func updateLastResyncEpoch(tx *bbolt.Tx, epoch uint64) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, epoch)

	b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
	if err != nil {
		return fmt.Errorf("can't create auxiliary bucket: %w", err)
	}
	return b.Put(lastResyncEpochKey, data)
}

func getLastResyncEpoch(tx *bbolt.Tx) uint64 {
	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(lastResyncEpochKey)
		if len(data) == 8 {
			return binary.LittleEndian.Uint64(data)
		}
	}

	return 0
}
