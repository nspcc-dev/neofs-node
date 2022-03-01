package meta

import (
	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"go.etcd.io/bbolt"
)

var (
	shardInfoBucket = []byte(invalidBase58String + "i")
	shardIDKey      = []byte("id")
)

// ReadShardID reads shard id from db.
// If id is missing, returns nil, nil.
func (db *DB) ReadShardID() ([]byte, error) {
	var id []byte
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(shardInfoBucket)
		if b != nil {
			id = slice.Copy(b.Get(shardIDKey))
		}
		return nil
	})
	return id, err
}

// WriteShardID writes shard it to db.
func (db *DB) WriteShardID(id []byte) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
		if err != nil {
			return err
		}
		return b.Put(shardIDKey, id)
	})
}
