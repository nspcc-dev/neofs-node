package meta

import (
	"bytes"
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
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/tzhash/tz"
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

// an older version of [updateCounter].
func updateCounter7Version(tx *bbolt.Tx, typ objectType, delta uint64, inc bool) error {
	b := tx.Bucket(shardInfoBucket)
	if b == nil {
		return nil
	}

	var counter uint64
	var counterKey []byte

	switch typ {
	case phyCounter:
		counterKey = objectPhyCounterKey
	case logicalCounter:
		counterKey = objectLogicCounterKey
	default:
		panic("unknown object type counter")
	}

	data := b.Get(counterKey)
	if len(data) == 8 {
		counter = binary.LittleEndian.Uint64(data)
	}

	if inc {
		counter += delta
	} else if counter <= delta {
		counter = 0
	} else {
		counter -= delta
	}

	newCounter := make([]byte, 8)
	binary.LittleEndian.PutUint64(newCounter, counter)

	return b.Put(counterKey, newCounter)
}

func (db *DB) InhumeContainer7Version(cID cid.ID) (uint64, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return 0, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return 0, ErrReadOnlyMode
	}

	var removedAvailable uint64

	resetContainerSize := func(tx *bbolt.Tx, cID cid.ID) error {
		infoBkt := tx.Bucket([]byte{unusedContainerVolumePrefix})
		if infoBkt == nil {
			return nil
		}
		cnrBkt := infoBkt.Bucket(cID[:])
		if cnrBkt == nil {
			return nil
		}
		err := cnrBkt.Put([]byte{containerStorageSizeKey}, make([]byte, 8))
		if err != nil {
			return fmt.Errorf("put zero storage size: %w", err)
		}
		err = cnrBkt.Put([]byte{containerObjectsNumberKey}, make([]byte, 8))
		if err != nil {
			return fmt.Errorf("put zero objects number: %w", err)
		}

		return nil
	}

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cID))
		if err != nil {
			return fmt.Errorf("create meta bucket: %w", err)
		}
		if err := metaBkt.Put(containerGCMarkKey, nil); err != nil {
			return fmt.Errorf("write container GC mark: %w", err)
		}

		info := db.containerInfo(tx, cID)
		removedAvailable = info.ObjectsNumber

		err = updateCounter7Version(tx, logicalCounter, removedAvailable, false)
		if err != nil {
			return fmt.Errorf("logical counter update: %w", err)
		}

		return resetContainerSize(tx, cID)
	})

	return removedAvailable, err
}

func getCounters7Version(tx *bbolt.Tx) (uint64, uint64) {
	var phyC, logicC uint64

	b := tx.Bucket(shardInfoBucket)
	if b != nil {
		data := b.Get(objectPhyCounterKey)
		if len(data) == 8 {
			phyC = binary.LittleEndian.Uint64(data)
		}

		data = b.Get(objectLogicCounterKey)
		if len(data) == 8 {
			logicC = binary.LittleEndian.Uint64(data)
		}
	}

	return phyC, logicC
}

func newDBBefore10Version(t testing.TB, opts ...Option) *DB {
	db := newDB(t, opts...)
	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte{unusedContainerVolumePrefix})
		if err != nil {
			return fmt.Errorf("create volume bucket: %w", err)
		}
		return nil
	})
	require.NoError(t, err)

	return db
}

func TestMigrate7to8(t *testing.T) {
	db := newDBBefore10Version(t)
	cnr := cidtest.ID()
	var totalSize uint64
	const objsNum = 10
	for range objsNum {
		err := db.boltDB.Update(func(tx *bbolt.Tx) error {
			o := objecttest.Object()
			o.SetContainerID(cnr)
			o.SetType(object.TypeRegular)
			totalSize += o.PayloadSize()

			require.NoError(t, updateCounter7Version(tx, phyCounter, 1, true))
			require.NoError(t, updateCounter7Version(tx, logicalCounter, 1, true))

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

			require.NoError(t, updateCounter7Version(tx, phyCounter, 1, true))

			return PutMetadataForObject(tx, o, true)
		})
		require.NoError(t, err)
	}
	_, err := db.InhumeContainer7Version(inhumeCnr)
	require.NoError(t, err)

	// one more parent (virtual) object
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		return PutMetadataForObject(tx, objecttest.Object(), false)
	})
	require.NoError(t, err)

	// force 7th version
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		infoBtk := tx.Bucket([]byte{unusedContainerVolumePrefix})
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
	err = migrateFrom7Version(db)
	require.NoError(t, err)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		infoBkt := tx.Bucket([]byte{unusedContainerVolumePrefix})
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
		require.Equal(t, []byte{0x08, 0, 0, 0, 0, 0, 0, 0}, bkt.Get([]byte("version")))

		pc, lc := getCounters7Version(tx)
		require.Equal(t, uint64(objsNum+inhumeObjsNum), pc)
		require.Equal(t, uint64(objsNum), lc)
		return nil
	})
	require.NoError(t, err)

	// Verify GetGarbage sees the container via meta marker (should list all objects)
	trash, err := db.GetGarbage(inhumeObjsNum + 5)
	require.NoError(t, err)
	require.Len(t, trash, 1)
	require.Equal(t, inhumeCnr, trash[0].Container)
	require.Len(t, trash[0].Objects, inhumeObjsNum)
}

type ObjectCounters8Version struct {
	logic uint64
	phy   uint64
}

func (o ObjectCounters8Version) Logic() uint64 {
	return o.logic
}

func (o ObjectCounters8Version) Phy() uint64 {
	return o.phy
}

func (db *DB) ObjectCounters8Version() (ObjectCounters8Version, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ObjectCounters8Version{}, ErrDegradedMode
	}

	var res ObjectCounters8Version
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		res.phy, res.logic = getCounters7Version(tx)
		return nil
	})

	return res, err
}

func TestMigrate8to9(t *testing.T) {
	db := newDBBefore10Version(t)

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
		if err := updateCounter7Version(tx, phyCounter, math.MaxUint64, false); err != nil {
			return err
		}
		if err := updateCounter7Version(tx, phyCounter, garbageCount, true); err != nil {
			return err
		}

		bkt, err := tx.CreateBucketIfNotExists([]byte{0x05})
		if err != nil {
			return err
		}

		return bkt.Put([]byte("version"), []byte{0x08, 0, 0, 0, 0, 0, 0, 0})
	})
	require.NoError(t, err)

	c, err := db.ObjectCounters8Version()
	require.NoError(t, err)
	require.EqualValues(t, garbageCount, c.Phy())

	// migrate
	require.NoError(t, migrateFrom8Version(db))

	// check counters have been corrected
	c, err = db.ObjectCounters8Version()
	require.NoError(t, err)
	require.EqualValues(t, phyContainerNum*phyObjectsPerContainer+parentObjects, c.Phy())
}

func TestMigrate9To10(t *testing.T) {
	cID := cidtest.ID()
	oTombstoned := objecttest.Object()
	oTombstoned.ResetRelations()
	oTombstoned.SetType(object.TypeRegular)
	oTombstoned.SetContainerID(cID)
	oTombstoned.SetPayloadSize(11)

	o := objecttest.Object()
	o.ResetRelations()
	o.SetType(object.TypeRegular)
	o.SetContainerID(cID)
	o.SetPayloadSize(22)

	ts := objecttest.Object()
	ts.ResetRelations()
	ts.SetType(object.TypeTombstone)
	ts.SetContainerID(cID)
	ts.AssociateDeleted(oTombstoned.GetID())
	ts.SetPayloadSize(33)

	link := objecttest.Object()
	link.ResetRelations()
	link.SetType(object.TypeLink)
	link.SetContainerID(cID)
	link.SetPayloadSize(44)

	lock := objecttest.Object()
	lock.ResetRelations()
	lock.SetType(object.TypeLock)
	lock.SetContainerID(cID)
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
		o.SetPayloadHomomorphicHash(checksum.NewTillichZemor([tz.Size]byte{}))
		if i < numOfTestObjs {
			o.SetContainerID(cID1)
		} else {
			o.SetContainerID(cID2)
		}

		objs = append(objs, o)
	}

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
}
