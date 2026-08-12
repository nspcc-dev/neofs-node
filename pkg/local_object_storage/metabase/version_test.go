package meta

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

type epochStateImpl struct{}

func (s epochStateImpl) CurrentEpoch() uint64 {
	return 0
}

func TestVersion(t *testing.T) {
	dir := t.TempDir()

	newDB := func(t *testing.T) *DB {
		return New(WithPath(filepath.Join(dir, t.Name())),
			WithPermissions(0o600), WithEpochState(epochStateImpl{}))
	}
	check := func(t *testing.T, db *DB) {
		require.NoError(t, db.boltDB.View(func(tx *bbolt.Tx) error {
			b := tx.Bucket(shardInfoBucket)
			if b == nil {
				return errors.New("shard info bucket not found")
			}
			data := b.Get(versionKey)
			if len(data) != 8 {
				return errors.New("invalid version data")
			}
			if stored := binary.LittleEndian.Uint64(data); stored != currentMetaVersion {
				return fmt.Errorf("invalid version: %d != %d", stored, currentMetaVersion)
			}
			return nil
		}))
	}
	t.Run("simple", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.Init(common.ID{}))
		check(t, db)
		require.NoError(t, db.Close())

		t.Run("reopen", func(t *testing.T) {
			require.NoError(t, db.Open(false))
			require.NoError(t, db.Init(common.ID{}))
			check(t, db)
			require.NoError(t, db.Close())
		})
	})
	t.Run("old data", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.WriteShardID([]byte{1, 2, 3, 4}))
		require.NoError(t, db.Close())

		require.NoError(t, db.Open(false))
		require.NoError(t, db.Init(common.ID{}))
		check(t, db)
		require.NoError(t, db.Close())
	})
	t.Run("invalid version", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.Open(false))
		require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
			return updateVersion(tx, currentMetaVersion+1)
		}))
		require.NoError(t, db.Close())

		require.NoError(t, db.Open(false))
		require.Error(t, db.Init(common.ID{}))
		require.NoError(t, db.Close())

		t.Run("reset", func(t *testing.T) {
			require.NoError(t, db.Open(false))
			require.NoError(t, db.Reset())
			check(t, db)
			require.NoError(t, db.Close())
		})
	})
}

type epochState uint64

func (s epochState) CurrentEpoch() uint64 { return uint64(s) }

func testEpochState(e uint64) *epochState {
	s := epochState(e)
	return &s
}

func newDB(t testing.TB, opts ...Option) *DB {
	p := path.Join(t.TempDir(), "meta.db")

	bdb := New(
		append([]Option{
			WithPath(p),
			WithPermissions(0o600),
			WithEpochState(testEpochState(123)),
			WithContainers(mockContainers{}),
		}, opts...)...,
	)

	require.NoError(t, bdb.Open(false))
	require.NoError(t, bdb.Init(common.ID{}))

	t.Cleanup(func() {
		bdb.Close()
		os.Remove(bdb.DumpInfo().Path)
	})

	return bdb
}

func generateTypedObject(cnr cid.ID, typ object.Type) object.Object {
	data := make([]byte, 32)
	_, _ = rand.Read(data)

	obj := object.New(cnr, usertest.ID())
	obj.SetID(oidtest.ID())
	obj.SetType(typ)
	obj.SetPayload(data)
	obj.SetPayloadSize(uint64(len(data)))
	obj.SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(data)))

	return *obj
}

func TestSlicesCloneNil(t *testing.T) {
	// not stated in docs, but migrateContainersToMetaBucket relies on this
	require.Nil(t, slices.Clone([]byte(nil)))
}

func TestMigrate9To10(t *testing.T) {
	cID := cidtest.ID()
	oTombstoned := generateTypedObject(cID, object.TypeRegular)
	oTombstoned.SetPayloadSize(11)

	o := generateTypedObject(cID, object.TypeRegular)
	o.SetPayloadSize(22)

	ts := generateTypedObject(cID, object.TypeTombstone)
	ts.AssociateDeleted(oTombstoned.GetID())
	ts.SetPayloadSize(33)

	link := generateTypedObject(cID, object.TypeLink)
	link.SetPayloadSize(44)

	lock := generateTypedObject(cID, object.TypeLock)
	lock.SetPayloadSize(55)

	// every object except tombstoned one
	var totalPayloadSize uint64
	totalPayloadSize += o.PayloadSize()
	totalPayloadSize += ts.PayloadSize()
	totalPayloadSize += link.PayloadSize()
	totalPayloadSize += lock.PayloadSize()

	db := newDB(t)

	require.NoError(t, db.boltDB.Update(func(tx *bbolt.Tx) error {
		// Put objects, no counters handling

		err := PutMetadataForObject(tx, o, true)
		if err != nil {
			return err
		}
		err = PutMetadataForObject(tx, oTombstoned, true)
		if err != nil {
			return err
		}
		metaB := tx.Bucket(metaBucketKey(cID))
		err = handleObjectWithAssociation(metaB, &CountersDiff{}, 0, ts)
		if err != nil {
			return err
		}
		err = PutMetadataForObject(tx, ts, true)
		if err != nil {
			return err
		}
		err = PutMetadataForObject(tx, link, true)
		if err != nil {
			return err
		}
		err = PutMetadataForObject(tx, lock, true)
		if err != nil {
			return err
		}

		// put outdated shard info values

		someUint64Val := make([]byte, 8)
		binary.LittleEndian.PutUint64(someUint64Val, 12345678)

		infoBkt := tx.Bucket(shardInfoBucket)
		err = infoBkt.Put(objectPhyCounterKey, slices.Clone(someUint64Val))
		if err != nil {
			return err
		}
		err = infoBkt.Put(objectLogicCounterKey, slices.Clone(someUint64Val))
		if err != nil {
			return err
		}

		// put deprecated container volume counters

		bVolume, err := tx.CreateBucketIfNotExists([]byte{unusedContainerVolumePrefix})
		if err != nil {
			return err
		}
		bCnr, err := bVolume.CreateBucket(cID[:])
		if err != nil {
			return err
		}
		err = bCnr.Put([]byte{containerStorageSizeKey}, someUint64Val)
		if err != nil {
			return err
		}
		err = bCnr.Put([]byte{containerObjectsNumberKey}, someUint64Val)
		if err != nil {
			return err
		}

		return nil
	}))

	require.NoError(t, migrateFrom9Version(db))

	require.NoError(t, db.boltDB.View(func(tx *bbolt.Tx) error {
		// there are no old values

		v := tx.Bucket(shardInfoBucket).Get(objectPhyCounterKey)
		require.Nil(t, v)
		v = tx.Bucket(shardInfoBucket).Get(objectLogicCounterKey)
		require.Nil(t, v)

		// there are actual resynced new counters

		requireUint64Value := func(v []byte, want uint64) {
			require.NotNil(t, v)

			require.Equal(t, want, binary.LittleEndian.Uint64(v))
		}

		metaB := tx.Bucket(metaBucketKey(cID))
		requireUint64Value(metaB.Get([]byte{metaPrefixPhyCounter}), 5)
		requireUint64Value(metaB.Get([]byte{metaPrefixRootCounter}), 2)
		requireUint64Value(metaB.Get([]byte{metaPrefixTSCounter}), 1)
		requireUint64Value(metaB.Get([]byte{metaPrefixLinkCounter}), 1)
		requireUint64Value(metaB.Get([]byte{metaPrefixLockCounter}), 1)
		requireUint64Value(metaB.Get([]byte{metaPrefixGCCounter}), 1)
		requireUint64Value(metaB.Get([]byte{metaPrefixPayloadCounter}), totalPayloadSize)

		// there is no container volume bucket

		b := tx.Bucket([]byte{unusedContainerVolumePrefix})
		require.Nil(t, b)

		return nil
	}))
}

//nolint:staticcheck // the whole tests is about checking deprecated values
func TestMigrate10To11(t *testing.T) {
	var (
		db   = newDB(t)
		cID1 = cidtest.ID()
		cID2 = cidtest.ID()
	)

	const numOfTestObjs = 2005 // a little more than single iteration in `updateContainersInterruptable` for two containers
	objs := make([]object.Object, 0, numOfTestObjs)
	for i := range numOfTestObjs {
		o := objecttest.Object()
		o.SetPayloadHomomorphicHash(checksum.New(checksum.TillichZemor, []byte("legacy TZ checksum")))
		if i < numOfTestObjs {
			o.SetContainerID(cID1)
		} else {
			o.SetContainerID(cID2)
		}

		objs = append(objs, o)
	}

	associatedTarget := oidtest.ID()
	associatedObj := objecttest.Object()
	associatedObj.SetContainerID(cID1)
	associatedObj.AssociateLocked(associatedTarget)

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		bkt1, err := tx.CreateBucketIfNotExists(metaBucketKey(cID1))
		require.NoError(t, err)
		bkt2, err := tx.CreateBucketIfNotExists(metaBucketKey(cID2))
		require.NoError(t, err)

		for _, o := range objs {
			err = PutMetadataForObject(tx, o, true)
			if err != nil {
				return err
			}
		}

		if err = PutMetadataForObject(tx, associatedObj, true); err != nil {
			return err
		}

		for i, o := range objs {
			var bkt *bbolt.Bucket
			if i < numOfTestObjs/2 {
				bkt = bkt1
			} else {
				bkt = bkt2
			}

			// copied from old `PutMetadataForObject` version with homomorphic hashes
			{
				var keyBuf keyBuffer
				if h, ok := o.PayloadHomomorphicHash(); ok {
					if err = putPlainAttribute(bkt, &keyBuf, o.GetID(), object.FilterPayloadHomomorphicHash, string(h.Value())); err != nil {
						return err
					}
				}
			}
		}

		newAttrIDKey := makeAssociatedAttrIDKey(associatedObj.GetID(), associatedTarget[:])
		newIDAttrKey := makeAssociatedIDAttrKey(associatedObj.GetID(), associatedTarget[:])
		require.NoError(t, bkt1.Delete(newAttrIDKey))
		require.NoError(t, bkt1.Delete(newIDAttrKey))
		require.NoError(t, bkt1.Put(makeAssociatedAttrIDKey(associatedObj.GetID(), []byte(associatedTarget.EncodeToString())), nil))
		require.NoError(t, bkt1.Put(makeAssociatedIDAttrKey(associatedObj.GetID(), []byte(associatedTarget.EncodeToString())), nil))

		return nil
	})
	require.NoError(t, err)

	countFields := func(db *bbolt.DB) (int, error) {
		var numOfFields int
		err := db.View(func(tx *bbolt.Tx) error {
			for _, cID := range []cid.ID{cID1, cID2} {
				b := tx.Bucket(metaBucketKey(cID))
				err = b.ForEach(func(_, _ []byte) error {
					numOfFields++
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return 0, err
		}

		return numOfFields, nil
	}

	numOfFieldsBefore, err := countFields(db.boltDB)
	require.NoError(t, err)

	err = updateContainersInterruptable(db, []byte{metadataPrefix}, dropHomomorphicIndexes)
	require.NoError(t, err)

	err = updateContainersInterruptable(db, []byte{metadataPrefix}, migrateAssociatedObjectValueToIDBytes)
	require.NoError(t, err)

	numOfFieldsAfter, err := countFields(db.boltDB)
	require.NoError(t, err)

	require.Equal(t, numOfFieldsBefore-2*numOfTestObjs, numOfFieldsAfter) // two indexes deleted for every object

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		for _, cID := range []cid.ID{cID1, cID2} {
			b := tx.Bucket(metaBucketKey(cID))
			c := b.Cursor()

			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				switch k[0] {
				case metaPrefixAttrIDPlain:
					if bytes.HasPrefix(k[1:], []byte(object.FilterPayloadHomomorphicHash)) {
						return fmt.Errorf("found ATTR -> ID key for %s container: %x", cID, k)
					}
				case metaPrefixIDAttr:
					if bytes.HasPrefix(k[1+oid.Size:], []byte(object.FilterPayloadHomomorphicHash)) {
						return fmt.Errorf("found ID -> ATTR key for %s container: %x", cID, k)
					}
				default:
				}
			}
		}

		return nil
	})
	require.NoError(t, err)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(metaBucketKey(cID1))
		c := b.Cursor()

		require.Equal(t, associatedTarget[:], getObjAttribute(c, associatedObj.GetID(), object.AttributeAssociatedObject))

		var collected []oid.ID
		for id := range iterAttrVal(c, object.AttributeAssociatedObject, associatedTarget[:]) {
			collected = append(collected, id)
		}
		require.Equal(t, []oid.ID{associatedObj.GetID()}, collected)

		for id := range iterAttrVal(c, object.AttributeAssociatedObject, []byte(associatedTarget.EncodeToString())) {
			t.Fatalf("unexpected legacy string index hit after migration: %s", id)
		}

		return nil
	})
	require.NoError(t, err)
}

func makeAssociatedAttrIDKey(id oid.ID, value []byte) []byte {
	res := make([]byte, 1+len(object.AttributeAssociatedObject)+1+len(value)+1+oid.Size)
	res[0] = metaPrefixAttrIDPlain
	off := 1 + copy(res[1:], object.AttributeAssociatedObject)
	res[off] = 0
	off++
	off += copy(res[off:], value)
	res[off] = 0
	off++
	copy(res[off:], id[:])
	return res
}

func makeAssociatedIDAttrKey(id oid.ID, value []byte) []byte {
	res := make([]byte, 1+oid.Size+len(object.AttributeAssociatedObject)+1+len(value))
	res[0] = metaPrefixIDAttr
	off := 1 + copy(res[1:], id[:])
	off += copy(res[off:], object.AttributeAssociatedObject)
	res[off] = 0
	off++
	copy(res[off:], value)
	return res
}
