package meta

import (
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
)

// version contains current metabase version.
const version = 0

var versionKey = []byte("version")

func checkVersion(tx *bbolt.Tx, initialized bool) error {
	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(versionKey)
		if len(data) == 8 {
			stored := binary.LittleEndian.Uint64(data)
			if stored != version {
				return fmt.Errorf("invalid version: expected=%d, stored=%d", version, stored)
			}
		}
	}
	if !initialized { // new database, write version
		return updateVersion(tx, version)
	}
	return nil // return error here after the first version increase
}

func updateVersion(tx *bbolt.Tx, version uint64) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, version)

	b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
	if err != nil {
		return fmt.Errorf("can't create auxilliary bucket: %w", err)
	}
	return b.Put(versionKey, data)
}
