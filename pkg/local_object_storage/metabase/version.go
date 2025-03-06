package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

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

func (db *DB) checkVersion() error {
	var stored uint64
	var knownVersion bool
	if err := db.boltDB.View(func(tx *bbolt.Tx) error {
		stored, knownVersion = getVersion(tx)
		return nil
	}); err != nil {
		return err
	}

	switch {
	case !knownVersion:
		// new database, write version
		return db.boltDB.Update(func(tx *bbolt.Tx) error { return updateVersion(tx, currentMetaVersion) })
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

		err := migrate(db)
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

var migrateFrom = map[uint64]func(*DB) error{
	2: migrateFrom2Version,
	3: migrateFrom3Version,
}

func migrateFrom2Version(db *DB) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error { return migrateFrom2VersionTx(tx, db.epochState) })
}

func migrateFrom2VersionTx(tx *bbolt.Tx, epochState EpochState) error {
	tsExpiration := epochState.CurrentEpoch() + objectconfig.DefaultTombstoneLifetime
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

func migrateFrom3Version(db *DB) error {
	var fromBkt, afterObj []byte
	for {
		select {
		case <-db.initCtx.Done():
			return context.Cause(db.initCtx)
		default:
		}
		if err := db.boltDB.Update(func(tx *bbolt.Tx) error {
			var err error
			if fromBkt, afterObj, err = migrateContainersToMetaBucket(db.log, db.cfg.containers, tx, fromBkt, afterObj); err == nil {
				fromBkt, afterObj = slices.Clone(fromBkt), slices.Clone(afterObj) // needed after the tx lifetime
			}
			return err
		}); err != nil {
			return err
		}
		if fromBkt == nil {
			break
		}
	}
	return db.boltDB.Update(func(tx *bbolt.Tx) error { return updateVersion(tx, 4) })
}

func migrateContainersToMetaBucket(l *zap.Logger, cs Containers, tx *bbolt.Tx, fromBkt, afterObj []byte) ([]byte, []byte, error) {
	c := tx.Cursor()
	var name []byte
	if fromBkt != nil {
		name, _ = c.Seek(fromBkt)
	} else {
		name, _ = c.First()
	}
	rem := uint(1000)
	var done uint
	var err error
	for ; name != nil; name, _ = c.Next() {
		switch name[0] {
		default:
			continue
		case primaryPrefix, tombstonePrefix, storageGroupPrefix, lockersPrefix, linkObjectsPrefix:
		}
		if len(name[1:]) != cid.Size {
			return nil, nil, fmt.Errorf("invalid container bucket with prefix 0x%X: wrong CID len %d", name[0], len(name[1:]))
		}
		cnr := cid.ID(name[1:])
		if exists, err := cs.Exists(cnr); err != nil {
			return nil, nil, fmt.Errorf("check container presence: %w", err)
		} else if !exists {
			l.Info("container no longer exists, ignoring", zap.Stringer("container", cnr))
			continue
		}
		b := tx.Bucket(name) // must not be nil, bbolt/Tx.ForEach follows the same assumption
		if done, afterObj, err = migrateContainerToMetaBucket(l, tx, b.Cursor(), cnr, afterObj, rem); err != nil {
			return nil, nil, fmt.Errorf("process container 0x%X%s bucket: %w", name[0], cnr, err)
		}
		if done == rem {
			break
		}
		rem -= done
	}
	return name, afterObj, nil
}

func migrateContainerToMetaBucket(l *zap.Logger, tx *bbolt.Tx, c *bbolt.Cursor, cnr cid.ID, after []byte, limit uint) (uint, []byte, error) {
	var k, v []byte
	if after != nil {
		if k, v = c.Seek(after); bytes.Equal(k, after) {
			k, v = c.Next()
		}
	} else {
		k, v = c.First()
	}
	metaBkt := tx.Bucket(metaBucketKey(cnr)) // may be nil
	var done uint
	for ; k != nil; k, v = c.Next() {
		ok, err := migrateObjectToMetaBucket(l, tx, metaBkt, cnr, k, v)
		if err != nil {
			return 0, nil, err
		}
		if ok {
			done++
			if done == limit {
				break
			}
		}
	}
	return done, k, nil
}

func migrateObjectToMetaBucket(l *zap.Logger, tx *bbolt.Tx, metaBkt *bbolt.Bucket, cnr cid.ID, id, bin []byte) (bool, error) {
	if len(id) != oid.Size {
		return false, fmt.Errorf("wrong OID key len %d", len(id))
	}
	var hdr object.Object
	if err := hdr.Unmarshal(bin); err != nil {
		l.Info("invalid object binary in the container bucket's value, ignoring", zap.Error(err),
			zap.Stringer("container", cnr), zap.Stringer("object", oid.ID(id)), zap.Binary("data", bin))
		return false, nil
	}
	if err := verifyHeaderForMetadata(hdr); err != nil {
		l.Info("invalid header in the container bucket, ignoring", zap.Error(err),
			zap.Stringer("container", cnr), zap.Stringer("object", oid.ID(id)), zap.Binary("data", bin))
		return false, nil
	}
	if metaBkt != nil {
		key := [1 + oid.Size]byte{metaPrefixID}
		copy(key[1:], id)
		if metaBkt.Get(key[:]) != nil {
			return false, nil
		}
	}
	par := hdr.Parent()
	hasParent := par != nil
	if err := putMetadataForObject(tx, hdr, hasParent, true); err != nil {
		return false, fmt.Errorf("put metadata for object %s: %w", oid.ID(id), err)
	}
	if hasParent && !par.GetID().IsZero() { // skip the first object without useful info similar to DB.put
		if err := verifyHeaderForMetadata(hdr); err != nil {
			l.Info("invalid parent header in the container bucket, ignoring", zap.Error(err),
				zap.Stringer("container", cnr), zap.Stringer("child", oid.ID(id)),
				zap.Stringer("parent", par.GetID()), zap.Binary("data", bin))
			return false, nil
		}
		if err := putMetadataForObject(tx, *par, false, false); err != nil {
			return false, fmt.Errorf("put metadata for parent of object %s: %w", oid.ID(id), err)
		}
	}
	return true, nil
}
