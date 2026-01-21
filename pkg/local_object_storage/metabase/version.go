package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/bbolt"
	berrors "github.com/nspcc-dev/bbolt/errors"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// currentMetaVersion contains current metabase version. It's incremented
// each time we have some changes to perform in metabase on upgrade, usually
// when there are some incompatibilities between old/new schemes of storing
// things, but sometimes data needs to be corrected and it's also a valid
// case for meta version update. Format changes and current scheme MUST be
// documented in VERSION.md.
const currentMetaVersion = 9

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
		5: migrateFrom5Version,
		6: migrateFrom6Version,
		7: migrateFrom7Version,
		8: migrateFrom8Version,
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

// garbageObjectsBucketName is pre-version-9 garbage bucket.
var garbageObjectsBucketName = []byte{unusedGarbageObjectsPrefix}

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

func migrateFrom7Version(db *DB) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		type cnrAndSize struct {
			cID     cid.ID
			sizeRaw []byte
		}

		// add GC marker key into meta buckets for containers already marked for GC
		garbageContainersBucketName := []byte{unusedGarbageContainersPrefix}
		garbageContainersBKT := tx.Bucket(garbageContainersBucketName)
		if garbageContainersBKT != nil {
			c := garbageContainersBKT.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if len(k) != cid.Size { // skip malformed key
					db.log.Warn("skip malformed key in garbage containers bucket while migration",
						zap.Int("key length", len(k)))
					continue
				}
				var cID cid.ID
				if err := cID.Decode(k); err != nil {
					db.log.Warn("skip malformed container ID in garbage containers bucket while migration",
						zap.Int("key length", len(k)), zap.Error(err))
					continue
				}
				metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cID))
				if err != nil {
					return fmt.Errorf("create meta bucket for GC-marked container %s: %w", cID, err)
				}
				if err := metaBkt.Put(containerGCMarkKey, nil); err != nil {
					return fmt.Errorf("write GC container marker for %s: %w", cID, err)
				}
			}

			err := tx.DeleteBucket(garbageContainersBucketName)
			if err != nil {
				return fmt.Errorf("deleting garbage containers bucket: %w", err)
			}
		}

		currEpoch := db.epochState.CurrentEpoch()
		phyPrefix := mkFilterPhysicalPrefix()
		infoBkt := tx.Bucket(containerVolumeBucketName)
		var cnrsOld []cnrAndSize
		err := infoBkt.ForEach(func(cnr, sizeRaw []byte) error {
			if sizeRaw != nil {
				cnrsOld = append(cnrsOld, cnrAndSize{cID: cid.ID(cnr), sizeRaw: sizeRaw})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("iterating container volumes: %w", err)
		}
		for _, cnrOld := range cnrsOld {
			err := infoBkt.Delete(cnrOld.cID[:])
			if err != nil {
				return fmt.Errorf("removing old values for %s container: %w", cnrOld.cID, err)
			}

			cnrBkt, err := infoBkt.CreateBucket(cnrOld.cID[:])
			if err != nil {
				return fmt.Errorf("creating (%s) container info bucket: %w", cnrOld.cID, err)
			}
			err = cnrBkt.Put([]byte{containerStorageSizeKey}, cnrOld.sizeRaw)
			if err != nil {
				return fmt.Errorf("put old container size to new %s container info bucket: %w", cnrOld.cID, err)
			}

			metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cnrOld.cID))
			if err != nil {
				return fmt.Errorf("get (%s) meta bucket: %w", cnrOld.cID, err)
			}
			var (
				metaCursor = metaBkt.Cursor()
				objsNumber uint64
			)
			k, _ := metaCursor.Seek(phyPrefix)
			for ; bytes.HasPrefix(k, phyPrefix); k, _ = metaCursor.Next() {
				id := oid.ID(k[len(phyPrefix):])
				if objectStatus(metaBkt.Cursor(), id, currEpoch) == statusAvailable {
					objsNumber++
				}
			}

			objsNumRaw := make([]byte, 8)
			binary.LittleEndian.PutUint64(objsNumRaw, objsNumber)
			err = cnrBkt.Put([]byte{containerObjectsNumberKey}, objsNumRaw)
			if err != nil {
				return fmt.Errorf("put new object size value for (%s) container: %w", cnrOld.cID, err)
			}
		}

		err = syncCounter(tx, true)
		if err != nil {
			return fmt.Errorf("syncing counters: %w", err)
		}

		return updateVersion(tx, 8)
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

func migrateFrom8Version(db *DB) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		var (
			err             error
			obsoleteBuckets = [][]byte{{unusedLockedPrefix}, {unusedGraveyardPrefix}, {unusedGarbageObjectsPrefix}, {unusedToMoveItPrefix}}
		)

		err = moveGarbageToMeta(db.log, tx)
		if err != nil {
			return fmt.Errorf("move garbage bucket keys: %w", err)
		}

		for _, name := range obsoleteBuckets {
			err = tx.DeleteBucket(name)
			if err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
				return fmt.Errorf("deleting %v bucket: %w", name, err)
			}
		}

		err = syncCounter(tx, true)
		if err != nil {
			return fmt.Errorf("resync object counters: %w", err)
		}

		return updateVersion(tx, 9)
	})
}

func moveGarbageToMeta(log *zap.Logger, tx *bbolt.Tx) error {
	var garbageBkt = tx.Bucket(garbageObjectsBucketName)

	if garbageBkt == nil {
		return nil
	}

	var garbageCursor = garbageBkt.Cursor()
	for k, _ := garbageCursor.First(); k != nil; k, _ = garbageCursor.Next() {
		if len(k) != addressKeySize {
			if len(k) > 2*addressKeySize {
				k = k[:2*addressKeySize] // don't spam in log
			}
			log.Warn("bad entry in garbage container", zap.String("k", hex.EncodeToString(k)))
			continue
		}

		var (
			bktKey  = append([]byte{metadataPrefix}, k[:cidSize]...)
			objKey  = append([]byte{metaPrefixGarbage}, k[cidSize:]...)
			metaBkt = tx.Bucket(bktKey)
		)
		if metaBkt == nil {
			log.Warn("no meta bucket found", zap.String("k", hex.EncodeToString(k)))
			continue
		}

		var err = metaBkt.Put(objKey, nil)
		if err != nil {
			return fmt.Errorf("put %s into %s: %w", hex.EncodeToString(objKey), hex.EncodeToString(bktKey), err)
		}
	}

	return nil
}
