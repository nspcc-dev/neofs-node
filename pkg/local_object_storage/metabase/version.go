package meta

import (
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"go.etcd.io/bbolt"
)

// version contains current metabase version.
const version = 2

var versionKey = []byte("version")

// ErrOutdatedVersion is returned on initializing
// an existing metabase that is not compatible with
// the current code version.
var ErrOutdatedVersion = logicerr.New("invalid version, resynchronization is required")

func checkVersion(tx *bbolt.Tx, initialized bool) error {
	var knownVersion bool

	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(versionKey)
		if len(data) == 8 {
			knownVersion = true

			stored := binary.LittleEndian.Uint64(data)
			if stored != version {
				return fmt.Errorf("%w: expected=%d, stored=%d", ErrOutdatedVersion, version, stored)
			}
		}
	}

	if !initialized {
		// new database, write version
		return updateVersion(tx, version)
	} else if !knownVersion {
		// db is initialized but no version
		// has been found; that could happen
		// if the db is corrupted or the version
		// is <2 (is outdated and requires resync
		// anyway)
		return ErrOutdatedVersion
	}

	return nil
}

func updateVersion(tx *bbolt.Tx, version uint64) error {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, version)

	b, err := tx.CreateBucketIfNotExists(shardInfoBucket)
	if err != nil {
		return fmt.Errorf("can't create auxiliary bucket: %w", err)
	}
	return b.Put(versionKey, data)
}
