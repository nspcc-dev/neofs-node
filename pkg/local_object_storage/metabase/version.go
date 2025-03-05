package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// currentMetaVersion contains current metabase version.
const currentMetaVersion = 4

var versionKey = []byte("version")

// ErrOutdatedVersion is returned on initializing
// an existing metabase that is not compatible with
// the current code version.
var ErrOutdatedVersion = logicerr.New("invalid version, resynchronization is required")

func (db *DB) checkVersion(tx *bbolt.Tx) error {
	stored, knownVersion := getVersion(tx)

	switch {
	case !knownVersion:
		// new database, write version
		return updateVersion(tx, currentMetaVersion)
	case stored == currentMetaVersion:
		return nil
	case stored > currentMetaVersion:
		return fmt.Errorf("%w: expected=%d, stored=%d", ErrOutdatedVersion, currentMetaVersion, stored)
	}

	// Outdated, but can be migrated.
	for i := stored; i < currentMetaVersion; i++ {
		migrate, ok := migrateFrom[i]
		if !ok {
			return fmt.Errorf("%w: expected=%d, stored=%d", ErrOutdatedVersion, currentMetaVersion, stored)
		}

		err := migrate(db, tx)
		if err != nil {
			return fmt.Errorf("migrating from meta version %d failed, consider database resync: %w", i, err)
		}
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

func getVersion(tx *bbolt.Tx) (uint64, bool) {
	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(versionKey)
		if len(data) == 8 {
			return binary.LittleEndian.Uint64(data), true
		}
	}

	return 0, false
}

var migrateFrom = map[uint64]func(*DB, *bbolt.Tx) error{
	2: migrateFrom2Version,
	3: migrateFrom3Version,
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

	return updateVersion(tx, 3)
}

func migrateFrom3Version(db *DB, tx *bbolt.Tx) error {
	c := tx.Cursor()
	pref := []byte{metadataPrefix}
	if k, _ := c.Seek(pref); bytes.HasPrefix(k, pref) {
		return fmt.Errorf("key with prefix 0x%X detected, metadata space is occupied by unexpected data or the version has not been updated to #%d", pref, currentMetaVersion)
	}
	err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
		switch name[0] {
		default:
			return nil
		case primaryPrefix, tombstonePrefix, storageGroupPrefix, lockersPrefix, linkObjectsPrefix:
		}
		if len(name[1:]) != cid.Size {
			return fmt.Errorf("invalid container bucket with prefix 0x%X: wrong CID len %d", name[0], len(name[1:]))
		}
		cnr := cid.ID(name[1:])
		err := b.ForEach(func(k, v []byte) error {
			if len(k) != oid.Size {
				return fmt.Errorf("wrong OID key len %d", len(k))
			}
			id := oid.ID(k)
			var hdr object.Object
			if err := hdr.Unmarshal(v); err != nil {
				db.log.Info("invalid object binary in the container bucket's value, ignoring", zap.Error(err),
					zap.Stringer("container", cnr), zap.Stringer("object", id), zap.Binary("data", v))
				return nil
			}
			if err := verifyHeaderForMetadata(hdr); err != nil {
				db.log.Info("invalid header in the container bucket, ignoring", zap.Error(err),
					zap.Stringer("container", cnr), zap.Stringer("object", id), zap.Binary("data", v))
				return nil
			}
			par := hdr.Parent()
			hasParent := par != nil
			if err := putMetadataForObject(tx, hdr, hasParent, true); err != nil {
				return fmt.Errorf("put metadata for object %s: %w", id, err)
			}
			if hasParent && !par.GetID().IsZero() { // skip the first object without useful info similar to DB.put
				if err := verifyHeaderForMetadata(hdr); err != nil {
					db.log.Info("invalid parent header in the container bucket, ignoring", zap.Error(err),
						zap.Stringer("container", cnr), zap.Stringer("child", id),
						zap.Stringer("parent", par.GetID()), zap.Binary("data", v))
					return nil
				}
				if err := putMetadataForObject(tx, *par, false, false); err != nil {
					return fmt.Errorf("put metadata for parent of object %s: %w", id, err)
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("process container 0x%X%s bucket: %w", name[0], cnr, err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return updateVersion(tx, 4)
}
