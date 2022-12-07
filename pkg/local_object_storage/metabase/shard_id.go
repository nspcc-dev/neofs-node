package meta

import (
	"runtime/debug"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"go.etcd.io/bbolt"
)

var (
	shardInfoBucket = []byte{shardInfoPrefix}
	shardIDKey      = []byte("id")
)

// ReadShardID reads shard id from db.
// If id is missing, returns nil, nil.
func (db *DB) ReadShardID() (id []byte, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	err = db.boltDB.View(func(tx *bbolt.Tx) (err error) {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer common.BboltFatalHandler(&err)

		b := tx.Bucket(shardInfoBucket)
		if b != nil {
			id = slice.Copy(b.Get(shardIDKey))
		}
		return nil
	})
	return id, err
}

// WriteShardID writes shard it to db.
func (db *DB) WriteShardID(id []byte) (err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	return db.boltDB.Update(func(tx *bbolt.Tx) (err error) {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer common.BboltFatalHandler(&err)

		b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
		if err != nil {
			return err
		}
		return b.Put(shardIDKey, id)
	})
}
