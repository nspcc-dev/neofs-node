package meta

import (
	"encoding/binary"
	"errors"
	"fmt"

	"go.etcd.io/bbolt"
)

// version contains current metabase version.
const version = 2

var versionKey = []byte("version")

// ErrOutdatedVersion is returned on initializing
// an existing metabase that is not compatible with
// the current code version.
var ErrOutdatedVersion = errors.New("invalid version")

func checkVersion(tx *bbolt.Tx, initialized bool) error {
	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(versionKey)
		if len(data) == 8 {
			stored := binary.LittleEndian.Uint64(data)
			if stored != version {
				return fmt.Errorf("%w: expected=%d, stored=%d", ErrOutdatedVersion, version, stored)
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
