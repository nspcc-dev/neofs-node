package meta

import (
	"encoding/binary"
	"errors"
	"fmt"

	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"go.etcd.io/bbolt"
)

// version contains current metabase version.
const version = 3

var versionKey = []byte("version")

// ErrOutdatedVersion is returned on initializing
// an existing metabase that is not compatible with
// the current code version.
var ErrOutdatedVersion = logicerr.New("invalid version, resynchronization is required")

func (db *DB) checkVersion(tx *bbolt.Tx) error {
	var (
		knownVersion bool
		migrated     bool
	)

	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(versionKey)
		if len(data) == 8 {
			knownVersion = true

			stored := binary.LittleEndian.Uint64(data)
			if stored != version {
				migrate, ok := migrateFrom[stored]
				if !ok {
					return fmt.Errorf("%w: expected=%d, stored=%d", ErrOutdatedVersion, version, stored)
				}

				err := migrate(db, tx)
				if err != nil {
					return fmt.Errorf("migrating from %d to %d version failed, consider database resync: %w", stored, version, err)
				}
				migrated = true
			}
		}
	}

	if !db.initialized || migrated {
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

func getVersion(tx *bbolt.Tx) uint64 {
	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(versionKey)
		if len(data) == 8 {
			return binary.LittleEndian.Uint64(data)
		}
	}

	return 0
}

var migrateFrom = map[uint64]func(*DB, *bbolt.Tx) error{
	2: migrateFrom2Version,
}

func migrateFrom2Version(db *DB, tx *bbolt.Tx) error {
	tsExpiration := db.epochState.CurrentEpoch() + objectconfig.DefaultTombstoneLifetime
	bkt := tx.Bucket(graveyardBucketName)
	if bkt == nil {
		return errors.New("graveyard bucket is nil")
	}
	c := bkt.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		l := len(v)
		if l == addressKeySize+8 { // Because of a 0.44.0 bug we can have a migrated DB with version 2.
			continue
		}
		if l != addressKeySize {
			return fmt.Errorf("graveyard value with unexpected %d length", l)
		}

		newVal := make([]byte, addressKeySize, addressKeySize+8)
		copy(newVal, v)
		newVal = binary.LittleEndian.AppendUint64(newVal, tsExpiration)

		err := bkt.Put(k, newVal)
		if err != nil {
			return err
		}
	}

	return nil
}
