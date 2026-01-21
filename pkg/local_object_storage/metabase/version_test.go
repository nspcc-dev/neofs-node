package meta

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nspcc-dev/bbolt"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
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
		require.NoError(t, db.Init())
		check(t, db)
		require.NoError(t, db.Close())

		t.Run("reopen", func(t *testing.T) {
			require.NoError(t, db.Open(false))
			require.NoError(t, db.Init())
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
		require.NoError(t, db.Init())
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
		require.Error(t, db.Init())
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
	require.NoError(t, bdb.Init())

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

func TestMigrate6to7(t *testing.T) {
	db := newDB(t)

	const cnrNum = 5
	const objsPerCnr = 20
	mObjs := make(map[cid.ID][]oid.ID)
	trashKey := []byte("trash")
	leakingObj := oidtest.ID()

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		garbageBkt, err := tx.CreateBucketIfNotExists([]byte{0x01})
		require.NoError(t, err)

		cnrs := cidtest.IDs(cnrNum)

		for _, cnr := range cnrs {
			metaBkt, err := tx.CreateBucketIfNotExists(slices.Concat([]byte{0xFF}, cnr[:]))
			require.NoError(t, err)

			ids := oidtest.IDs(objsPerCnr)

			for j, id := range ids {
				metaKey := slices.Concat([]byte{0x00}, id[:])
				require.NoError(t, metaBkt.Put(metaKey, nil))

				var garbageKey []byte
				if j%2 == 0 { // correct
					garbageKey = id[:]
				} else { // broken
					garbageKey = slices.Concat(cnr[:], id[:])
				}

				require.NoError(t, garbageBkt.Put(garbageKey, nil))
			}

			mObjs[cnr] = ids
		}

		// add trash key which is neither OID nor CID+OID
		require.NoError(t, garbageBkt.Put(trashKey, nil))
		// add random OID which should be cleaned because there is no container for it
		require.NoError(t, garbageBkt.Put(leakingObj[:], nil))

		// force old version
		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.NoError(t, bkt.Put([]byte("version"), []byte{0x06, 0, 0, 0, 0, 0, 0, 0}))

		return nil
	})
	require.NoError(t, err)

	// migrate
	require.NoError(t, db.Init())

	gObjs, _, err := db.GetGarbage(cnrNum*objsPerCnr + 2)
	require.NoError(t, err)
	require.EqualValues(t, cnrNum*objsPerCnr, len(gObjs))

	for cnr, ids := range mObjs {
		for _, id := range ids {
			require.Contains(t, gObjs, oid.NewAddress(cnr, id))
		}
	}

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		// check new version
		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.Equal(t, []byte{currentMetaVersion, 0, 0, 0, 0, 0, 0, 0}, bkt.Get([]byte("version")))

		return nil
	})
	require.NoError(t, err)
}

func TestMigrate7to8(t *testing.T) {
	db := newDB(t)
	cnr := cidtest.ID()
	var totalSize uint64
	const objsNum = 10
	for range objsNum {
		err := db.boltDB.Update(func(tx *bbolt.Tx) error {
			o := objecttest.Object()
			o.SetContainerID(cnr)
			o.SetType(object.TypeRegular)
			totalSize += o.PayloadSize()

			require.NoError(t, updateCounter(tx, phy, 1, true))
			require.NoError(t, updateCounter(tx, logical, 1, true))

			return PutMetadataForObject(tx, o, true)
		})
		require.NoError(t, err)
	}

	inhumeCnr := cidtest.ID()
	const inhumeObjsNum = 5
	for range inhumeObjsNum {
		err := db.boltDB.Update(func(tx *bbolt.Tx) error {
			o := objecttest.Object()
			o.SetContainerID(inhumeCnr)

			require.NoError(t, updateCounter(tx, phy, 1, true))

			return PutMetadataForObject(tx, o, true)
		})
		require.NoError(t, err)
	}
	_, err := db.InhumeContainer(inhumeCnr)
	require.NoError(t, err)

	// one more parent (virtual) object
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		return PutMetadataForObject(tx, objecttest.Object(), false)
	})
	require.NoError(t, err)

	// force 7th version
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		infoBtk := tx.Bucket(containerVolumeBucketName)
		buff := make([]byte, 8)
		binary.LittleEndian.PutUint64(buff, totalSize)
		err = infoBtk.Put(cnr[:], buff)
		if err != nil {
			return err
		}

		// corrupt counters
		b := tx.Bucket(shardInfoBucket)
		require.NotNil(t, b)
		require.Equal(t, uint64(objsNum+inhumeObjsNum), binary.LittleEndian.Uint64(b.Get(objectPhyCounterKey)))
		require.Equal(t, uint64(objsNum), binary.LittleEndian.Uint64(b.Get(objectLogicCounterKey)))
		cc := make([]byte, 8)
		binary.LittleEndian.PutUint64(cc, uint64(100)) // corrupt physical counter
		require.NoError(t, b.Put(objectPhyCounterKey, cc))
		cc = make([]byte, 8)
		binary.LittleEndian.PutUint64(cc, uint64(100)) // corrupt logical counter
		require.NoError(t, b.Put(objectLogicCounterKey, cc))

		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.NoError(t, bkt.Put([]byte("version"), []byte{0x07, 0, 0, 0, 0, 0, 0, 0}))

		return nil
	})
	require.NoError(t, err)

	// migrate
	require.NoError(t, db.Init())

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		infoBkt := tx.Bucket(containerVolumeBucketName)
		v := infoBkt.Get(cnr[:])
		require.Nil(t, v) // it is a bucket now, now a regular value

		cnrBkt := infoBkt.Bucket(cnr[:])
		require.NotNil(t, cnrBkt)

		sizeRaw := cnrBkt.Get([]byte{containerStorageSizeKey})
		sizeRead := binary.LittleEndian.Uint64(sizeRaw)
		require.Equal(t, totalSize, sizeRead)

		objsNumRaw := cnrBkt.Get([]byte{containerObjectsNumberKey})
		objsNumRead := binary.LittleEndian.Uint64(objsNumRaw)
		require.Equal(t, uint64(objsNum), objsNumRead)

		// verify GC meta marker present
		metaBkt := tx.Bucket(metaBucketKey(inhumeCnr))
		require.NotNil(t, metaBkt)
		require.NotNil(t, metaBkt.Get([]byte{4}))

		// check new version
		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.Equal(t, []byte{currentMetaVersion, 0, 0, 0, 0, 0, 0, 0}, bkt.Get([]byte("version")))

		pc, lc := getCounters(tx)
		require.Equal(t, uint64(objsNum+inhumeObjsNum), pc)
		require.Equal(t, uint64(objsNum), lc)
		return nil
	})
	require.NoError(t, err)

	// Verify GetGarbage sees the container via meta marker (should list all objects and list the container)
	gObjs, gCnrs, err := db.GetGarbage(inhumeObjsNum + 5)
	require.NoError(t, err)
	require.Len(t, gObjs, inhumeObjsNum)
	require.Len(t, gCnrs, 1)
	require.Equal(t, inhumeCnr, gCnrs[0])
}

func TestMigrate8to9(t *testing.T) {
	db := newDB(t)

	// store several root phy objects
	const phyContainerNum = 5
	const phyObjectsPerContainer = 10

	anyOwner := usertest.ID()
	anyChecksum := checksumtest.Checksum()

	for range phyContainerNum {
		cnr := cidtest.ID()

		for range phyObjectsPerContainer {
			var obj object.Object
			obj.SetContainerID(cnr)
			obj.SetID(oidtest.ID())
			obj.SetOwner(anyOwner)
			obj.SetPayloadChecksum(anyChecksum)

			require.NoError(t, db.Put(&obj))
		}
	}

	// store several phy objects with parent
	const parentObjects = 3
	for range parentObjects {
		var parent object.Object
		parent.SetContainerID(cidtest.ID())
		parent.SetID(oidtest.ID())
		parent.SetOwner(anyOwner)
		parent.SetPayloadChecksum(anyChecksum)

		var child object.Object
		child.SetContainerID(parent.GetContainerID())
		child.SetID(oidtest.ID())
		child.SetOwner(anyOwner)
		child.SetPayloadChecksum(anyChecksum)
		child.SetParent(&parent)

		require.NoError(t, db.Put(&child))
	}

	// force incorrect counters and previous DB version
	const garbageCount = 1234567890

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		// reset to zero
		if err := updateCounter(tx, phy, math.MaxUint64, false); err != nil {
			return err
		}
		if err := updateCounter(tx, phy, garbageCount, true); err != nil {
			return err
		}

		bkt, err := tx.CreateBucketIfNotExists([]byte{0x05})
		if err != nil {
			return err
		}

		return bkt.Put([]byte("version"), []byte{0x08, 0, 0, 0, 0, 0, 0, 0})
	})
	require.NoError(t, err)

	c, err := db.ObjectCounters()
	require.NoError(t, err)
	require.EqualValues(t, garbageCount, c.Phy())

	// migrate
	require.NoError(t, db.Init())

	// check counters have been corrected
	c, err = db.ObjectCounters()
	require.NoError(t, err)
	require.EqualValues(t, phyContainerNum*phyObjectsPerContainer+parentObjects, c.Phy())
}
