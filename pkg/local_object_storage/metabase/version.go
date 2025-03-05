package meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

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

func GetVersion(db *DB) (uint64, error) {
	var res uint64
	return res, db.boltDB.View(func(tx *bbolt.Tx) error {
		res, _ = getVersion(tx)
		return nil
	})
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

func migrateFrom3Version(_ *DB, tx *bbolt.Tx) error {
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
				return fmt.Errorf("decode header of object %s from bucket value: %w", id, err)
			}
			par := hdr.Parent()
			hasParent := par != nil
			if err := putMetadataForObject(tx, hdr, hasParent, true); err != nil {
				return fmt.Errorf("put metadata for object %s: %w", id, err)
			}
			if hasParent && !par.GetID().IsZero() { // skip the first object without useful info similar to DB.put
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

func MigrateFrom3Version(ctx context.Context, db *DB, objsPerTx uint) error {
	db.log.Info("storing objects' metadata in the metabucket...", zap.Uint("batch size", objsPerTx))
	if err := migrateToMetaBucket(ctx, db, objsPerTx); err != nil {
		return fmt.Errorf("store objects' metadata in the metabucket: %w", err)
	}
	db.log.Info("objects' metadata successfully stored in the meta bucket, updating metabase version...")
	// TODO: doing this within the last batch TX can be a bit more efficient
	if err := db.boltDB.Update(func(tx *bbolt.Tx) error { return updateVersion(tx, 4) }); err != nil {
		return fmt.Errorf("BoltDB transaction updating metabase version failure: %w", err)
	}
	return nil
}

func migrateToMetaBucket(ctx context.Context, db *DB, objsPerTx uint) error {
	var skipped, stored, invalidBin, invalidProto, diffID uint
	var lastCnr, lastObj []byte
	metaBktName := [1 + cid.Size]byte{metadataPrefix}
	metaBktOIDKey := [1 + oid.Size]byte{metaPrefixID}
	st := time.Now()
	for txNum := 1; ; txNum++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("interrupted by context: %w", context.Cause(ctx))
		default:
		}
		var storedTx uint
		err := db.boltDB.Update(func(tx *bbolt.Tx) error {
			seekCnrPhase := lastCnr != nil
			err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
				switch name[0] {
				default:
					return nil
				case primaryPrefix, tombstonePrefix, storageGroupPrefix, lockersPrefix, linkObjectsPrefix:
				}
				if len(name[1:]) != cid.Size {
					return fmt.Errorf("invalid container bucket with prefix 0x%X: wrong CID len %d", name[0], len(name[1:]))
				}

				if seekCnrPhase && !bytes.Equal(name[1:], lastCnr) { // container has already been processed
					return nil
				}
				c := b.Cursor()
				var k, v []byte
				if seekCnrPhase && lastObj != nil {
					if k, v = c.Seek(lastObj); bytes.Equal(k, lastObj) {
						k, v = c.Next()
					}
				} else {
					k, v = c.First()
				}
				seekCnrPhase = false

				cnr := cid.ID(name[1:])
				copy(metaBktName[1:], cnr[:])
				metaBkt := tx.Bucket(metaBktName[:])
				for ; k != nil; k, v = c.Next() {
					if len(k) != oid.Size {
						return fmt.Errorf("wrong OID key len %d", len(k))
					}
					id := oid.ID(k)
					var hdr object.Object
					if err := hdr.Unmarshal(v); err != nil {
						db.log.Error("invalid object binary in the container bucket's value, ignoring", zap.Error(err),
							zap.Stringer("container", cnr), zap.Stringer("object", id))
						// print DB items in debug mode only, they can be huge
						db.log.Debug("invalid object binary in the container bucket", zap.Binary("data", v))
						invalidBin++
						continue
					}
					if inHdr := hdr.GetID(); inHdr != id {
						db.log.Error("ID in header binary differs from the container bucket's key, ignoring",
							zap.Stringer("container", cnr), zap.Stringer("in header", inHdr), zap.Stringer("key", id))
						// print DB items in debug mode only, they can be huge
						db.log.Debug("header binary with diff ID", zap.Uint8("prefix", name[0]), zap.Binary("data", v))
						diffID++
						continue
					}
					if metaBkt != nil {
						if copy(metaBktOIDKey[1:], id[:]); metaBkt.Get(metaBktOIDKey[:]) != nil {
							skipped++
							continue
						}
					}
					par := hdr.Parent()
					hasParent := par != nil
					if err := putMetadataForObject(tx, hdr, hasParent, true); err != nil {
						if errors.Is(err, errDB) { // TODO: invert to non-DB
							return fmt.Errorf("put metadata for object %s: %w", id, err)
						}
						db.log.Error("incorrect object in the container bucket, ignoring", zap.Error(err),
							zap.Stringer("container", cnr), zap.Stringer("object", id))
						invalidProto++
						continue
					}
					if hasParent && !par.GetID().IsZero() { // skip the first object without useful info similar to DB.put
						if err := putMetadataForObject(tx, *par, false, false); err != nil {
							if errors.Is(err, errDB) { // TODO: invert to non-DB
								return fmt.Errorf("put metadata for parent of object %s: %w", id, err)
							}
							db.log.Error("incorrect parent object in the container bucket, ignoring", zap.Error(err),
								zap.Stringer("container", cnr), zap.Stringer("object", par.GetID()), zap.Stringer("child", id))
							invalidProto++
							continue
						}
					}
					// note that we count child and parent objects as 1 to not complicate the continuation logic.
					// This happens not very often, so the batch won't exceed the limit much
					if storedTx++; storedTx == objsPerTx {
						lastCnr, lastObj = slices.Clone(name[1:]), slices.Clone(k) // go beyond the TX
						return ErrInterruptIterator
					}
				}
				return nil
			})
			if !errors.Is(err, ErrInterruptIterator) {
				return err
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("BoltDB transaction saving next object batch in the metabucket failure: %w", err)
		}
		stored += storedTx
		db.log.Info("next TX", zap.Uint("stored", stored))
		if storedTx < objsPerTx {
			break
		}
	}
	db.log.Debug("meta bucket migration stats", zap.Uint("batch size", objsPerTx), zap.Stringer("took", time.Since(st)), zap.Uint("stored", stored),
		zap.Uint("done earlier", skipped), zap.Uint("invalid binary", invalidBin), zap.Uint("invalid proto", invalidProto),
		zap.Uint("diff ID", diffID))
	return nil
}
