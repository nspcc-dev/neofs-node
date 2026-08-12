package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/bbolt"
	berrors "github.com/nspcc-dev/bbolt/errors"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// currentMetaVersion contains current metabase version. It's incremented
// each time we have some changes to perform in metabase on upgrade, usually
// when there are some incompatibilities between old/new schemes of storing
// things, but sometimes data needs to be corrected and it's also a valid
// case for meta version update. Format changes and current scheme MUST be
// documented in VERSION.md.
const currentMetaVersion = 11

var (
	// migrateFrom stores migration callbacks for respective versions.
	// They're executed sequentially as needed and each function is
	// expected to upgrade exactly to the next version. If current version
	// is 5 and some metabase is of version 3 it'd run 3->4 and 4->5
	// migration functions. We don't always store all migration functions,
	// once all networks are upgraded they're hardly useful, so we only
	// need to maintain some "current" set of them, old ones need to be
	// deleted eventually.
	//
	// Upgrades can take a lot of time and they're interrupting the
	// service, so there are important things to consider wrt how these
	// functions work. If some DB iterations and a lot of changes to
	// specific key-value pairs are needed then the process should be
	// performed in batches of ~1000 KV pairs and be interruptible by
	// regular INT/TERM signals. There are already wrappers in code that
	// do this and they shouldn't be removed even if current code is not
	// using them. Special care should be taken for error handling. While
	// it's very tempting to refuse updating a broken DB when we detect
	// an inconsistency of some kind, for users this means a total SN DoS
	// and it's hardly acceptable, so in general it's better to log and
	// continue rather than return an error.
	migrateFrom = map[uint64]func(*DB) error{
		9:  migrateFrom9Version,
		10: migrateFrom10Version,
	}

	versionKey = []byte("version")
)

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

// nolint:unused
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

// nolint:unused
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

func migrateFrom9Version(db *DB) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		err := tx.DeleteBucket([]byte{unusedContainerVolumePrefix})
		if err != nil {
			if !errors.Is(err, berrors.ErrBucketNotFound) {
				return fmt.Errorf("deleting deprecated container volume bucket: %w", err)
			}
		}

		err = syncCounter(tx, true)
		if err != nil {
			return fmt.Errorf("resync object counters: %w", err)
		}

		infoBkt := tx.Bucket(shardInfoBucket)
		err = infoBkt.Delete(objectLogicCounterKey)
		if err != nil {
			return fmt.Errorf("delete old object logic counter: %w", err)
		}
		err = infoBkt.Delete(objectPhyCounterKey)
		if err != nil {
			return fmt.Errorf("delete old object phy counter: %w", err)
		}

		return updateVersion(tx, 10)
	})
}

func migrateFrom10Version(db *DB) error {
	err := updateContainersInterruptable(db, []byte{metadataPrefix}, dropHomomorphicIndexes)
	if err != nil {
		return fmt.Errorf("drop homomorphic indexes: %w", err)
	}

	err = updateContainersInterruptable(db, []byte{metadataPrefix}, migrateAssociatedObjectValueToIDBytes)
	if err != nil {
		return fmt.Errorf("rewrite %q attribute values in metadata: %w", object.AttributeAssociatedObject, err)
	}

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		err := syncCounter(tx, true)
		if err != nil {
			return fmt.Errorf("resync object counters: %w", err)
		}
		return updateVersion(tx, 11)
	})
}

func dropHomomorphicIndexes(_ *zap.Logger, _ *bbolt.Tx, b *bbolt.Bucket, _ cid.ID, _ []byte, limit uint) (uint, []byte, error) {
	var (
		c            = b.Cursor()
		k            []byte
		attrIDPrefix = []byte{metaPrefixAttrIDPlain}
		attrKeyLen   = len([]byte(object.FilterPayloadHomomorphicHash))
		keysToDrop   [][]byte
	)
	attrIDPrefix = append(attrIDPrefix, []byte(object.FilterPayloadHomomorphicHash)...)
	k, _ = c.Seek(attrIDPrefix)
	for ; bytes.HasPrefix(k, attrIDPrefix); k, _ = c.Next() {
		keysToDrop = append(keysToDrop, k)
		if len(keysToDrop) == int(limit) {
			break
		}
	}

	for _, keyToDrop := range keysToDrop {
		_ = b.Delete(keyToDrop)

		v := keyToDrop[1+attrKeyLen+len(objectcore.MetaAttributeDelimiter) : len(keyToDrop)-(oid.Size+len(objectcore.MetaAttributeDelimiter))]
		id := keyToDrop[len(keyToDrop)-oid.Size:]
		reversedKey := slices.Concat([]byte{metaPrefixIDAttr}, id, []byte(object.FilterPayloadHomomorphicHash), objectcore.MetaAttributeDelimiter, v)
		_ = b.Delete(reversedKey)
	}

	return uint(len(keysToDrop)), nil, nil
}

func migrateAssociatedObjectValueToIDBytes(l *zap.Logger, _ *bbolt.Tx, b *bbolt.Bucket, cnr cid.ID, afterKey []byte, rem uint) (uint, []byte, error) {
	c := b.Cursor()
	pref := append(append([]byte{metaPrefixAttrIDPlain}, object.AttributeAssociatedObject...), 0)

	k, _ := c.Seek(pref)
	if afterKey != nil {
		k, _ = c.Seek(afterKey)
		if bytes.Equal(k, afterKey) {
			k, _ = c.Next()
		}
	}

	type keyRewrite struct {
		oldAttrID []byte
		newAttrID []byte
		oldIDAttr []byte
		newIDAttr []byte
	}

	var (
		scanned uint
		nextKey []byte
		buf     keyBuffer
		updates []keyRewrite
	)

	for ; k != nil && bytes.HasPrefix(k, pref); k, _ = c.Next() {
		nextKey = slices.Clone(k)

		val, idRaw, err := splitAttributeValueObjectID(k[len(pref):])
		if err != nil {
			l.Warn("skip malformed associated object metadata entry during migration",
				zap.Stringer("container", cnr),
				zap.String("key", hex.EncodeToString(k)),
				zap.Error(err))
			continue
		}

		if _, err = oid.DecodeBytes(val); err == nil {
			continue
		}

		var associated oid.ID
		if err = associated.DecodeString(string(val)); err != nil {
			l.Warn("skip malformed associated object metadata entry during migration",
				zap.Stringer("container", cnr),
				zap.String("key", hex.EncodeToString(k)),
				zap.Error(err))
			continue
		}

		var id oid.ID
		copy(id[:], idRaw)

		newAttrID, off := prepareMetaAttrIDKey(&buf, id, object.AttributeAssociatedObject, oid.Size, false)
		copy(newAttrID[off:], associated[:])
		newAttrID = slices.Clone(newAttrID)
		oldIDAttr := prepareMetaIDAttrKey(&buf, id, object.AttributeAssociatedObject, len(val))
		copy(oldIDAttr[len(oldIDAttr)-len(val):], val)
		oldIDAttr = slices.Clone(oldIDAttr)
		newIDAttr := prepareMetaIDAttrKey(&buf, id, object.AttributeAssociatedObject, oid.Size)
		copy(newIDAttr[len(newIDAttr)-oid.Size:], associated[:])
		newIDAttr = slices.Clone(newIDAttr)

		updates = append(updates, keyRewrite{
			oldAttrID: slices.Clone(k),
			newAttrID: newAttrID,
			oldIDAttr: oldIDAttr,
			newIDAttr: newIDAttr,
		})
		scanned++

		if scanned == rem {
			break
		}
	}

	for i := range updates {
		if err := b.Put(updates[i].newAttrID, nil); err != nil {
			return 0, nil, fmt.Errorf("put migrated attribute-to-ID key %s: %w", hex.EncodeToString(updates[i].newAttrID), err)
		}
		if err := b.Put(updates[i].newIDAttr, nil); err != nil {
			return 0, nil, fmt.Errorf("put migrated ID-to-attribute key %s: %w", hex.EncodeToString(updates[i].newIDAttr), err)
		}
		if err := b.Delete(updates[i].oldAttrID); err != nil {
			return 0, nil, fmt.Errorf("delete migrated attribute-to-ID key %s: %w", hex.EncodeToString(updates[i].oldAttrID), err)
		}
		if err := b.Delete(updates[i].oldIDAttr); err != nil {
			return 0, nil, fmt.Errorf("delete migrated ID-to-attribute key %s: %w", hex.EncodeToString(updates[i].oldIDAttr), err)
		}
	}

	if scanned < rem {
		nextKey = nil
	}

	return scanned, nextKey, nil
}

func splitAttributeValueObjectID(b []byte) ([]byte, []byte, error) {
	if len(b) < oid.Size+1 {
		return nil, nil, fmt.Errorf("too short len %d", len(b))
	}

	valEnd := len(b) - oid.Size - 1
	if b[valEnd] != 0 {
		return nil, nil, errors.New("wrong value-object delimiter")
	}

	return b[:valEnd], b[valEnd+1:], nil
}
