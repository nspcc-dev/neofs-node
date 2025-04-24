package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// currentMetaVersion contains current metabase version.
const currentMetaVersion = 5

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
	4: migrateFrom4Version,
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
	var validPrefixes = []byte{primaryPrefix, tombstonePrefix, storageGroupPrefix, lockersPrefix, linkObjectsPrefix}

	err := updateContainersInterruptable(db, validPrefixes, migrateContainerToMetaBucket)
	if err != nil {
		return err
	}
	return db.boltDB.Update(func(tx *bbolt.Tx) error { return updateVersion(tx, 4) })
}

func updateContainersInterruptable(db *DB, validPrefixes []byte, migrationFunc func(*zap.Logger, *bbolt.Tx, *bbolt.Bucket, cid.ID, []byte, uint) (uint, []byte, error)) error {
	var fromBkt, afterObj []byte
	for {
		select {
		case <-db.initCtx.Done():
			return context.Cause(db.initCtx)
		default:
		}
		if err := db.boltDB.Update(func(tx *bbolt.Tx) error {
			var err error
			fromBkt, afterObj, err = iterateContainerBuckets(db.log, db.cfg.containers, tx, fromBkt, afterObj,
				validPrefixes, migrationFunc)
			if err == nil {
				fromBkt, afterObj = slices.Clone(fromBkt), slices.Clone(afterObj) // needed after the tx lifetime
			}
			return err
		}); err != nil {
			return err
		}
		if fromBkt == nil {
			return nil
		}
	}
}

func iterateContainerBuckets(l *zap.Logger, cs Containers, tx *bbolt.Tx, fromBkt []byte, afterObj []byte, validPrefixes []byte,
	migrationFunc func(*zap.Logger, *bbolt.Tx, *bbolt.Bucket, cid.ID, []byte, uint) (uint, []byte, error)) ([]byte, []byte, error) {
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
		if !slices.Contains(validPrefixes, name[0]) {
			continue
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
		if done, afterObj, err = migrationFunc(l, tx, b, cnr, afterObj, rem); err != nil {
			return nil, nil, fmt.Errorf("process container 0x%X%s bucket: %w", name[0], cnr, err)
		}
		if done == rem {
			break
		}
		rem -= done
	}
	return name, afterObj, nil
}

func migrateContainerToMetaBucket(l *zap.Logger, tx *bbolt.Tx, b *bbolt.Bucket, cnr cid.ID, after []byte, limit uint) (uint, []byte, error) {
	var (
		k, v []byte
		c    = b.Cursor()
	)
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
	if err := objectcore.VerifyHeaderForMetadata(hdr); err != nil {
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
	if err := PutMetadataForObject(tx, hdr, true); err != nil {
		return false, fmt.Errorf("put metadata for object %s: %w", oid.ID(id), err)
	}
	if par != nil && !par.GetID().IsZero() { // skip the first object without useful info similar to DB.put
		if err := objectcore.VerifyHeaderForMetadata(hdr); err != nil {
			l.Info("invalid parent header in the container bucket, ignoring", zap.Error(err),
				zap.Stringer("container", cnr), zap.Stringer("child", oid.ID(id)),
				zap.Stringer("parent", par.GetID()), zap.Binary("data", bin))
			return false, nil
		}
		if err := PutMetadataForObject(tx, *par, false); err != nil {
			return false, fmt.Errorf("put metadata for parent of object %s: %w", oid.ID(id), err)
		}
	}
	return true, nil
}

func migrateFrom4Version(db *DB) error {
	err := updateContainersInterruptable(db, []byte{metadataPrefix}, fixMiddleObjectRoots)
	if err != nil {
		return err
	}
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		var (
			buckets          [][]byte
			obsoletePrefixes = []byte{unusedSmallPrefix, unusedOwnerPrefix,
				unusedUserAttributePrefix, unusedPayloadHashPrefix,
				unusedSplitPrefix, unusedFirstObjectIDPrefix}
		)
		err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			if slices.Contains(obsoletePrefixes, name[0]) {
				buckets = append(buckets, slices.Clone(name))
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("iterating buckets: %w", err)
		}
		for _, name := range buckets {
			err := tx.DeleteBucket(name)
			if err != nil {
				return fmt.Errorf("deleting %v bucket: %w", name, err)
			}
		}
		return updateVersion(tx, 5)
	})
}

// fixMiddleObjectRoots removes root object marks for middle split objects, they
// were erroneously treated as root ones before the fix.
func fixMiddleObjectRoots(l *zap.Logger, tx *bbolt.Tx, b *bbolt.Bucket, cnr cid.ID, after []byte, limit uint) (uint, []byte, error) {
	var (
		c            = b.Cursor()
		k            []byte
		oidKey       = make([]byte, addressKeySize)
		attrIDPrefix = []byte{metaPrefixAttrIDPlain}
		queue        = make([]oid.ID, 0, limit)
	)
	attrIDPrefix = append(attrIDPrefix, object.FilterRoot...)
	attrIDPrefix = append(attrIDPrefix, objectcore.MetaAttributeDelimiter...)
	attrIDPrefix = append(attrIDPrefix, binPropMarker...)
	attrIDPrefix = append(attrIDPrefix, objectcore.MetaAttributeDelimiter...)
	if after == nil {
		after = attrIDPrefix
	}

	k, _ = c.Seek(after)
	if bytes.Equal(k, after) {
		k, _ = c.Next()
	}

	for ; bytes.HasPrefix(k, attrIDPrefix); k, _ = c.Next() {
		id, err := oid.DecodeBytes(k[len(attrIDPrefix):])
		if err != nil {
			return 0, nil, fmt.Errorf("wrong OID: %w", err)
		}

		var addr = oid.NewAddress(cnr, id)
		hdr, err := get(tx, addr, oidKey, false, false, 0)
		if err != nil {
			return 0, nil, fmt.Errorf("header error for %s: %w", addr, err)
		}

		if hdr.HasParent() {
			queue = append(queue, id)
			if len(queue) == int(limit) {
				break
			}
		}
	}
	for _, id := range queue {
		err := b.Delete(slices.Concat(attrIDPrefix, id[:]))
		if err != nil {
			l.Warn("failed to delete root attrID key", zap.Stringer("container", cnr), zap.Stringer("object", id))
		}
		idAttrKey := prepareMetaIDAttrKey(&keyBuffer{}, id, object.FilterRoot, len(binPropMarker))
		copy(idAttrKey[len(idAttrKey)-len(binPropMarker):], binPropMarker)
		err = b.Delete(idAttrKey)
		if err != nil {
			l.Warn("failed to delete root IDattr key", zap.Stringer("container", cnr), zap.Stringer("object", id))
		}
	}
	if len(queue) < int(limit) {
		k = nil // End of iteration for this bucket.
	}
	return uint(len(queue)), k, nil
}
