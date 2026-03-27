package meta

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
)

var (
	shardInfoBucket = []byte{shardInfoPrefix}
	shardIDKey      = []byte("id")
)

// ReadShardID reads shard id from db.
// If id is missing, returns nil, nil.
func (db *DB) ReadShardID() ([]byte, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	var id []byte
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(shardInfoBucket)
		if b != nil {
			id = bytes.Clone(b.Get(shardIDKey))
		}
		return nil
	})
	return id, err
}

// WriteShardID writes shard it to db.
func (db *DB) WriteShardID(id []byte) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
		if err != nil {
			return err
		}
		return b.Put(shardIDKey, id)
	})
}

func (db *DB) ensureShardID(id common.ID) error {
	if id.IsZero() {
		return nil
	}

	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
		if err != nil {
			return err
		}

		bStoredID := b.Get(shardIDKey)
		if bStoredID == nil {
			err = b.Put(shardIDKey, id.Bytes())
			if err != nil {
				return fmt.Errorf("store shard ID: %w", err)
			}
			return nil
		}
		storedID, err := common.NewIDFromBytes(bStoredID)
		if err != nil {
			return fmt.Errorf("invalid shard ID in metabase: %w", err)
		}
		if storedID != id {
			return fmt.Errorf("shard ID mismatch: in-metabase=%s, expected=%s", storedID, id)
		}
		return nil
	})
}
