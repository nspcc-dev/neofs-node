package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/mr-tron/base58"
	objectconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/object"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// currentMetaVersion contains current metabase version. It's incremented
// each time we have some changes to perform in metabase on upgrade, usually
// when there are some incompatibilities between old/new schemes of storing
// things, but sometimes data needs to be corrected and it's also a valid
// case for meta version update. Format changes and current scheme MUST be
// documented in VERSION.md.
const currentMetaVersion = 7

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
		2: migrateFrom2Version,
		3: migrateFrom3Version,
		4: migrateFrom4Version,
		5: migrateFrom5Version,
		6: migrateFrom6Version,
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
	var validPrefixes = []byte{unusedPrimaryPrefix, unusedTombstonePrefix, unusedStorageGroupPrefix, unusedLockersPrefix, unusedLinkObjectsPrefix}

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
				unusedSplitPrefix, unusedFirstObjectIDPrefix,
				unusedParentPrefix, unusedRootPrefix}
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
		hdr, err := getCompat(tx, addr, oidKey, false, 0)
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

// getCompat is used for migrations only, it retrieves full headers from
// respective buckets.
func getCompat(tx *bbolt.Tx, addr oid.Address, key []byte, raw bool, currEpoch uint64) (*object.Object, error) {
	key = objectKey(addr.Object(), key)
	cnr := addr.Container()
	obj := object.New()
	bucketName := make([]byte, bucketKeySize)

	// check in primary index
	data := getFromBucket(tx, primaryBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in tombstone index
	data = getFromBucket(tx, tombstoneBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in storage group index
	data = getFromBucket(tx, storageGroupBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in locker index
	data = getFromBucket(tx, bucketNameLockers(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in link objects index
	data = getFromBucket(tx, linkObjectsBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check if object is a virtual one, but this contradicts raw flag
	if raw {
		return nil, getSplitInfoError(tx, cnr, addr.Object(), bucketName)
	}
	return getVirtualObject(tx, cnr, addr.Object(), bucketName)
}

func getFromBucket(tx *bbolt.Tx, name, key []byte) []byte {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return nil
	}

	return bkt.Get(key)
}

func getChildForParent(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID) oid.ID {
	var (
		childOID     oid.ID
		metaBucket   = tx.Bucket(metaBucketKey(cnr))
		parentPrefix = getParentMetaOwnersPrefix(parentID)
	)

	if metaBucket == nil {
		return childOID
	}

	var cur = metaBucket.Cursor()
	k, _ := cur.Seek(parentPrefix)

	if bytes.HasPrefix(k, parentPrefix) {
		// Error will lead to zero oid which is ~the same as missing child.
		childOID, _ = oid.DecodeBytes(k[len(parentPrefix):])
	}
	return childOID
}

func getVirtualObject(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID, bucketName []byte) (*object.Object, error) {
	var childOID = getChildForParent(tx, cnr, parentID)

	if childOID.IsZero() {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	// we should have a link object
	data := getFromBucket(tx, linkObjectsBucketName(cnr, bucketName), childOID[:])
	if len(data) == 0 {
		// no link object, so we may have the last object with parent header
		data = getFromBucket(tx, primaryBucketName(cnr, bucketName), childOID[:])
	}

	if len(data) == 0 {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	child := object.New()

	err := child.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal %s child with %s parent: %w", childOID, parentID, err)
	}

	par := child.Parent()

	if par == nil { // this should never happen though
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	return par, nil
}

func getSplitInfoError(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID, bucketName []byte) error {
	var metaBucket = tx.Bucket(metaBucketKey(cnr))

	if metaBucket == nil {
		return logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	splitInfo, err := getSplitInfo(metaBucket, metaBucket.Cursor(), cnr, parentID)
	if err == nil {
		return logicerr.Wrap(object.NewSplitInfoError(splitInfo))
	}

	return logicerr.Wrap(apistatus.ObjectNotFound{})
}

// primaryBucketName returns <CID>.
func primaryBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, unusedPrimaryPrefix, key)
}

// returns name of the bucket with objects of type LOCK for specified container.
func bucketNameLockers(idCnr cid.ID, key []byte) []byte {
	return bucketName(idCnr, unusedLockersPrefix, key)
}

// tombstoneBucketName returns <CID>_TS.
func tombstoneBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, unusedTombstonePrefix, key)
}

// linkObjectsBucketName returns link objects bucket key (`18<CID>`).
func linkObjectsBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, unusedLinkObjectsPrefix, key)
}

// storageGroupBucketName returns <CID>_SG.
func storageGroupBucketName(cnr cid.ID, key []byte) []byte {
	return bucketName(cnr, unusedStorageGroupPrefix, key)
}

func migrateFrom5Version(db *DB) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		var (
			buckets          [][]byte
			obsoletePrefixes = []byte{unusedPrimaryPrefix,
				unusedLockersPrefix, unusedStorageGroupPrefix,
				unusedTombstonePrefix, unusedLinkObjectsPrefix}
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
		return updateVersion(tx, 6)
	})
}

func migrateFrom6Version(db *DB) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		if garbageBkt := tx.Bucket(garbageObjectsBucketName); garbageBkt != nil {
			if err := fixGarbageBucketKeys(db.log, tx, garbageBkt); err != nil {
				return fmt.Errorf("fix garbage bucket keys: %w", err)
			}
		}
		return updateVersion(tx, 7)
	})
}

func fixGarbageBucketKeys(log *zap.Logger, tx *bbolt.Tx, garbageBkt *bbolt.Bucket) error {
	var rmKeys [][]byte
	newItems := make(map[cid.ID][][]byte)
	metaOIDKey := [1 + oid.Size]byte{metaPrefixID}

	garbageCursor := garbageBkt.Cursor()
	for k, _ := garbageCursor.First(); k != nil; k, _ = garbageCursor.Next() {
		if len(k) != oid.Size {
			continue
		}

		copy(metaOIDKey[1:], k)

		cnr, err := resolveContainerByOID(tx, metaOIDKey[:])
		if err != nil {
			return fmt.Errorf("resolve container by OID: %w", err)
		}

		// update after so as not to break the cursor
		rmKeys = append(rmKeys, k)

		if cnr.IsZero() {
			log.Info("failed to resolve container for broken item in garbage bucket, removing...", zap.String("OID", base58.Encode(k)))
			continue
		}

		newItems[cnr] = append(newItems[cnr], k)
	}

	for i := range rmKeys {
		if err := garbageBkt.Delete(rmKeys[i]); err != nil {
			return fmt.Errorf("remove broken item: %w", err)
		}
	}

	var newKey [cid.Size + oid.Size]byte
	for cnr, objs := range newItems {
		copy(newKey[:], cnr[:])

		for _, id := range objs {
			copy(newKey[cid.Size:], id)

			if err := garbageBkt.Put(newKey[:], zeroValue); err != nil {
				return fmt.Errorf("put fixed item: %w", err)
			}
		}
	}

	return nil
}
