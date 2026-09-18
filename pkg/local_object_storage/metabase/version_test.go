package meta

import (
	"bytes"
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

func TestSlicesCloneNil(t *testing.T) {
	// not stated in docs, but migrateContainersToMetaBucket relies on this
	require.Nil(t, slices.Clone([]byte(nil)))
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
