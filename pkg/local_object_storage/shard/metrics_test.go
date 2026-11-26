package shard_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

type metricsStore struct {
	objectCounters map[string]uint64
	containerSize  map[cid.ID]int64
	payloadSize    int64
	readOnly       bool
}

func (m metricsStore) SetShardID(_ string) {}

func (m metricsStore) SetObjectCounter(objectType string, v uint64) {
	m.objectCounters[objectType] = v
}

func (m metricsStore) AddToObjectCounter(objectType string, delta int) {
	// Wrap instead of clamping so that a negative counter fails assertions.
	m.objectCounters[objectType] = uint64(int64(m.objectCounters[objectType]) + int64(delta))
}

func (m metricsStore) IncObjectCounter(objectType string) {
	m.objectCounters[objectType] += 1
}

func (m metricsStore) DecObjectCounter(objectType string) {
	m.AddToObjectCounter(objectType, -1)
}

func (m *metricsStore) SetReadonly(r bool) {
	m.readOnly = r
}

func (m metricsStore) AddToContainerSize(cnr string, size int64) {
	c, err := cid.DecodeString(cnr)
	if err != nil {
		return
	}
	m.containerSize[c] += size
}

func (m *metricsStore) AddToPayloadSize(size int64) {
	m.payloadSize += size
}

const (
	phyObj  = "phy"
	rootObj = "root"
	tsObj   = "ts"
	lockObj = "lock"
	linkObj = "link"
	gcObj   = "gc"
)

func TestCounters(t *testing.T) {
	dir := t.TempDir()
	sh, mm := shardWithMetrics(t, dir)

	err := sh.SetMode(mode.ReadOnly)
	require.NoError(t, err)
	require.True(t, mm.readOnly)
	err = sh.SetMode(mode.ReadWrite)
	require.NoError(t, err)
	require.False(t, mm.readOnly)

	const objNumber = 10
	oo := make([]*object.Object, objNumber)
	for i := range objNumber {
		oo[i] = generateObject()
		addPayload(oo[i], 100+i)
	}

	t.Run("defaults", func(t *testing.T) {
		require.Zero(t, mm.objectCounters[phyObj])
		require.Zero(t, mm.objectCounters[rootObj])
		require.Zero(t, mm.objectCounters[tsObj])
		require.Zero(t, mm.objectCounters[lockObj])
		require.Zero(t, mm.objectCounters[linkObj])
		require.Zero(t, mm.objectCounters[gcObj])
		require.Empty(t, mm.containerSize)
		require.Zero(t, mm.payloadSize)
	})

	var totalPayload int64

	expectedSizes := make(map[cid.ID]int64)
	for i := range oo {
		cnr := oo[i].GetContainerID()
		oSize := int64(oo[i].PayloadSize())
		expectedSizes[cnr] += oSize
		totalPayload += oSize
	}

	t.Run("put", func(t *testing.T) {
		for i := range objNumber {
			err := sh.Put(oo[i], nil)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(objNumber), mm.objectCounters[phyObj])
		require.Equal(t, uint64(objNumber), mm.objectCounters[rootObj])
		require.Zero(t, mm.objectCounters[tsObj])
		require.Zero(t, mm.objectCounters[lockObj])
		require.Zero(t, mm.objectCounters[linkObj])
		require.Zero(t, mm.objectCounters[gcObj])
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload, mm.payloadSize)
	})

	t.Run("inhume_GC", func(t *testing.T) {
		inhumedNumber := objNumber / 4

		for i := range inhumedNumber {
			err := sh.MarkGarbage(oo[i].GetContainerID(), []oid.ID{oo[i].GetID()}, meta.GarbageMarkDefault)
			require.NoError(t, err)

			expectedSizes[oo[i].GetContainerID()] -= int64(oo[i].PayloadSize())
			totalPayload -= int64(oo[i].PayloadSize())
		}

		require.Equal(t, uint64(objNumber), mm.objectCounters[phyObj])
		require.Equal(t, uint64(objNumber), mm.objectCounters[rootObj])
		require.Equal(t, uint64(inhumedNumber), mm.objectCounters[gcObj])
		require.Zero(t, mm.objectCounters[tsObj])
		require.Zero(t, mm.objectCounters[lockObj])
		require.Zero(t, mm.objectCounters[linkObj])
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload, mm.payloadSize)

		oo = oo[inhumedNumber:]
	})

	t.Run("inhume_TS", func(t *testing.T) {
		tombObj := objecttest.Object()
		tombObj.ResetRelations()
		tombObj.SetPayload(nil)
		tombObj.SetPayloadSize(0)
		tombObj.SetContainerID(oo[0].GetContainerID())
		tombObj.AssociateDeleted(oo[0].GetID())

		phy := mm.objectCounters[phyObj]
		root := phy
		inhumed := mm.objectCounters[gcObj]

		inhumedNumber := 1

		err := sh.Put(&tombObj, nil)
		require.NoError(t, err)

		// Tomb is an object too.
		phy++
		expectedSizes[oo[0].GetContainerID()] -= int64(oo[0].PayloadSize())
		totalPayload -= int64(oo[0].PayloadSize())

		require.Equal(t, phy, mm.objectCounters[phyObj])
		require.Equal(t, root, mm.objectCounters[rootObj])
		require.Equal(t, inhumed+1, mm.objectCounters[gcObj])
		require.Equal(t, uint64(1), mm.objectCounters[tsObj])
		require.Zero(t, mm.objectCounters[lockObj])
		require.Zero(t, mm.objectCounters[linkObj])
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload, mm.payloadSize)

		oo = oo[inhumedNumber:]
	})

	t.Run("Delete", func(t *testing.T) {
		phy := mm.objectCounters[phyObj]
		root := mm.objectCounters[rootObj]

		deletedNumber := int(phy / 4)

		for _, obj := range oo[:deletedNumber] {
			err := sh.Delete(obj.GetContainerID(), []oid.ID{obj.GetID()})
			require.NoError(t, err)
		}

		require.Equal(t, phy-uint64(deletedNumber), mm.objectCounters[phyObj])
		require.Equal(t, root-uint64(deletedNumber), mm.objectCounters[rootObj])
		require.Zero(t, mm.objectCounters[linkObj])
		require.Zero(t, mm.objectCounters[lockObj])
		var totalRemovedpayload uint64
		for i := range oo[:deletedNumber] {
			removedPayload := oo[i].PayloadSize()
			totalRemovedpayload += removedPayload

			expectedSizes[oo[i].GetContainerID()] -= int64(removedPayload)
		}
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload-int64(totalRemovedpayload), mm.payloadSize)

		t.Run("parent", func(t *testing.T) {
			cnr := cidtest.ID()

			parent := generateObjectWithCID(cnr)

			child := generateObjectWithCID(cnr)
			child.SetParent(parent)

			require.NoError(t, sh.Put(child, nil))

			require.EqualValues(t, child.PayloadSize(), mm.containerSize[cnr])

			cnrInfo, err := sh.ContainerInfo(cnr)
			require.NoError(t, err)
			require.EqualValues(t, child.PayloadSize(), cnrInfo.StorageSize)
			require.EqualValues(t, 1, cnrInfo.ObjectsNumber)

			phyBefore := mm.objectCounters[phyObj]
			sumPldSizeBefore := mm.payloadSize

			require.NoError(t, sh.Delete(cnr, []oid.ID{parent.GetID()}))

			// Deleting a virtual parent removes its physical child as well.
			require.Zero(t, mm.containerSize[cnr])
			require.Equal(t, phyBefore-1, mm.objectCounters[phyObj])
			require.Equal(t, sumPldSizeBefore-int64(child.PayloadSize()), mm.payloadSize)

			cnrInfo, err = sh.ContainerInfo(cnr)
			require.NoError(t, err)
			require.Zero(t, cnrInfo.StorageSize)
			require.Zero(t, cnrInfo.ObjectsNumber)
		})
	})
}

func TestInhumeContainerCounters(t *testing.T) {
	dir := t.TempDir()
	sh, mm := shardWithMetrics(t, dir)

	c1 := cidtest.ID()
	c2 := cidtest.ID()

	const objsC1, objsC2 = 5, 7
	var sizeC1, sizeC2 int64
	var idsC1 []oid.ID

	for i := range objsC1 {
		obj := generateObjectWithPayload(c1, make([]byte, 10+i))
		obj.SetPayloadSize(uint64(len(obj.Payload())))
		require.NoError(t, sh.Put(obj, nil))
		sizeC1 += int64(obj.PayloadSize())
		idsC1 = append(idsC1, obj.GetID())
	}
	for i := range objsC2 {
		obj := generateObjectWithPayload(c2, make([]byte, 20+i))
		obj.SetPayloadSize(uint64(len(obj.Payload())))
		require.NoError(t, sh.Put(obj, nil))
		sizeC2 += int64(obj.PayloadSize())
	}

	const total = objsC1 + objsC2
	check := func(t *testing.T, mm *metricsStore, phy, root, gc int, payload int64, cnrSizes map[cid.ID]int64) {
		require.Equal(t, map[string]uint64{
			phyObj:  uint64(phy),
			rootObj: uint64(root),
			gcObj:   uint64(gc),
			tsObj:   0,
			lockObj: 0,
			linkObj: 0,
		}, mm.objectCounters)
		require.Equal(t, payload, mm.payloadSize)
		require.Equal(t, cnrSizes, mm.containerSize)
	}
	// the metrics a shard would start with, given what it stores now
	checkRestarted := func(t *testing.T, phy, root, gc int, payload int64, cnrSizes map[cid.ID]int64) {
		require.NoError(t, sh.Close())
		sh, mm = shardWithMetrics(t, dir)
		check(t, mm, phy, root, gc, payload, cnrSizes)
	}

	payload := mm.payloadSize
	check(t, mm, total, total, 0, payload, map[cid.ID]int64{c1: sizeC1, c2: sizeC2})

	require.NoError(t, sh.InhumeContainer(c1))

	// objects are stored until GC deletes them, as garbage; their payload
	// is no longer the container's
	check(t, mm, total, total, objsC1, payload-sizeC1, map[cid.ID]int64{c1: 0, c2: sizeC2})

	// nothing changes the second time
	require.NoError(t, sh.InhumeContainer(c1))
	check(t, mm, total, total, objsC1, payload-sizeC1, map[cid.ID]int64{c1: 0, c2: sizeC2})

	checkRestarted(t, total, total, objsC1, sizeC2, map[cid.ID]int64{c1: 0, c2: sizeC2})

	// GC deletes some of them, the payload is not taken off again
	const deleted = 2
	require.NoError(t, sh.Delete(c1, idsC1[:deleted]))
	check(t, mm, total-deleted, total-deleted, objsC1-deleted, sizeC2, map[cid.ID]int64{c1: 0, c2: sizeC2})

	checkRestarted(t, total-deleted, total-deleted, objsC1-deleted, sizeC2, map[cid.ID]int64{c1: 0, c2: sizeC2})

	// and the rest after the restart
	require.NoError(t, sh.Delete(c1, idsC1[deleted:]))
	check(t, mm, objsC2, objsC2, 0, sizeC2, map[cid.ID]int64{c1: 0, c2: sizeC2})

	checkRestarted(t, objsC2, objsC2, 0, sizeC2, map[cid.ID]int64{c1: 0, c2: sizeC2})

	require.NoError(t, sh.InhumeContainer(c2))
	check(t, mm, objsC2, objsC2, objsC2, 0, map[cid.ID]int64{c1: 0, c2: 0})

	checkRestarted(t, objsC2, objsC2, objsC2, 0, map[cid.ID]int64{c1: 0, c2: 0})
}

func TestZeroPayloadContainerSize(t *testing.T) {
	t.Run("zero-byte put", func(t *testing.T) {
		dir := t.TempDir()
		sh, mm := shardWithMetrics(t, dir)

		cnr := cidtest.ID()
		obj := generateObjectWithPayload(cnr, nil)
		obj.SetPayloadSize(0)
		require.NoError(t, sh.Put(obj, nil))

		require.Equal(t, map[cid.ID]int64{cnr: 0}, mm.containerSize)
		require.Zero(t, mm.payloadSize)

		require.NoError(t, sh.Close())
		_, mm = shardWithMetrics(t, dir)
		require.Equal(t, map[cid.ID]int64{cnr: 0}, mm.containerSize)
		require.Zero(t, mm.payloadSize)
	})

	t.Run("inhume absent container", func(t *testing.T) {
		sh, mm := shardWithMetrics(t, t.TempDir())

		require.NoError(t, sh.InhumeContainer(cidtest.ID()))
		require.Empty(t, mm.containerSize)
		require.Zero(t, mm.payloadSize)
	})
}

func TestTombstoneCounters(t *testing.T) {
	t.Run("absent target", func(t *testing.T) {
		sh, mm := shardWithMetrics(t, t.TempDir())
		cnr := cidtest.ID()
		target := oidtest.ID()

		ts := generateObjectWithCID(cnr)
		ts.AssociateDeleted(target)
		require.NoError(t, sh.Put(ts, nil))
		require.Equal(t, uint64(1), mm.objectCounters[gcObj])

		require.NoError(t, sh.Delete(cnr, []oid.ID{target, ts.GetID()}))
		require.Zero(t, mm.objectCounters[gcObj])
		require.Zero(t, mm.objectCounters[phyObj])
	})

	t.Run("garbage target", func(t *testing.T) {
		sh, mm := shardWithMetrics(t, t.TempDir())
		cnr := cidtest.ID()

		obj := generateObjectWithCID(cnr)
		addPayload(obj, 100)
		require.NoError(t, sh.Put(obj, nil))
		require.NoError(t, sh.MarkGarbage(cnr, []oid.ID{obj.GetID()}, meta.GarbageMarkDefault))
		require.Zero(t, mm.payloadSize)
		require.Zero(t, mm.containerSize[cnr])

		ts := generateObjectWithCID(cnr)
		ts.AssociateDeleted(obj.GetID())
		require.NoError(t, sh.Put(ts, nil))
		require.Equal(t, uint64(1), mm.objectCounters[gcObj])
		require.Zero(t, mm.payloadSize)
		require.Zero(t, mm.containerSize[cnr])
	})
}

func TestReviveCounters(t *testing.T) {
	cnr := cidtest.ID()
	newTombstone := func(target oid.ID) *object.Object {
		tombObj := objecttest.Object()
		tombObj.ResetRelations()
		tombObj.SetPayload(nil)
		tombObj.SetPayloadSize(0)
		tombObj.SetContainerID(cnr)
		tombObj.AssociateDeleted(target)
		return &tombObj
	}

	alive := map[string]uint64{
		phyObj:  1,
		rootObj: 1,
		tsObj:   0,
		lockObj: 0,
		linkObj: 0,
		gcObj:   0,
	}

	setup := func(t *testing.T) (string, *shard.Shard, *metricsStore, *object.Object, int64) {
		t.Helper()
		dir := t.TempDir()
		sh, mm := shardWithMetrics(t, dir)
		obj := generateObjectWithCID(cnr)
		addPayload(obj, 100)
		require.NoError(t, sh.Put(obj, nil))
		return dir, sh, mm, obj, int64(obj.PayloadSize())
	}

	check := func(t *testing.T, mm *metricsStore, counters map[string]uint64, payload int64) {
		t.Helper()
		require.Equal(t, counters, mm.objectCounters)
		require.Equal(t, map[cid.ID]int64{cnr: payload}, mm.containerSize)
		require.Equal(t, payload, mm.payloadSize)
	}
	checkRestarted := func(t *testing.T, dir string, sh *shard.Shard, counters map[string]uint64, payload int64) (*shard.Shard, *metricsStore) {
		t.Helper()
		require.NoError(t, sh.Close())
		sh, mm := shardWithMetrics(t, dir)
		check(t, mm, counters, payload)
		return sh, mm
	}

	t.Run("from garbage", func(t *testing.T) {
		dir, sh, mm, obj, size := setup(t)

		require.NoError(t, sh.MarkGarbage(cnr, []oid.ID{obj.GetID()}, meta.GarbageMarkDefault))
		require.Equal(t, uint64(1), mm.objectCounters[gcObj])
		require.Zero(t, mm.containerSize[cnr])
		require.Zero(t, mm.payloadSize)

		_, err := sh.ReviveObject(obj.Address())
		require.NoError(t, err)

		check(t, mm, alive, size)
		checkRestarted(t, dir, sh, alive, size)
	})

	t.Run("from tombstone", func(t *testing.T) {
		dir, sh, mm, obj, size := setup(t)

		require.NoError(t, sh.Put(newTombstone(obj.GetID()), nil))

		require.Equal(t, uint64(2), mm.objectCounters[phyObj])
		require.Equal(t, uint64(1), mm.objectCounters[tsObj])
		require.Equal(t, uint64(1), mm.objectCounters[gcObj])
		require.Zero(t, mm.containerSize[cnr])
		require.Zero(t, mm.payloadSize)

		_, err := sh.ReviveObject(obj.Address())
		require.NoError(t, err)

		check(t, mm, alive, size)
		checkRestarted(t, dir, sh, alive, size)
	})

	t.Run("two tombstones", func(t *testing.T) {
		dir, sh, mm, obj, size := setup(t)

		ts1 := newTombstone(obj.GetID())
		ts2 := newTombstone(obj.GetID())
		require.NoError(t, sh.Put(ts1, nil))
		require.NoError(t, sh.Put(ts2, nil))

		rs, err := sh.ReviveObject(obj.Address())
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGraveyard, rs.StatusType())
		require.ElementsMatch(t, []oid.Address{ts1.Address(), ts2.Address()}, rs.TombstoneAddresses())

		exists, err := sh.Exists(obj.Address(), false)
		require.NoError(t, err)
		require.True(t, exists)
		for _, ts := range []*object.Object{ts1, ts2} {
			exists, err = sh.Exists(ts.Address(), false)
			require.NoError(t, err)
			require.False(t, exists)
		}

		check(t, mm, alive, size)
		checkRestarted(t, dir, sh, alive, size)
	})

	t.Run("target deleted by GC", func(t *testing.T) {
		dir, sh, mm, obj, _ := setup(t)

		other := generateObjectWithCID(cnr)
		addPayload(other, 50)
		require.NoError(t, sh.Put(other, nil))
		require.NoError(t, sh.MarkGarbage(cnr, []oid.ID{other.GetID()}, meta.GarbageMarkDefault))
		require.NoError(t, sh.Put(newTombstone(obj.GetID()), nil))

		require.NoError(t, sh.Delete(cnr, []oid.ID{obj.GetID()}))

		_, err := sh.ReviveObject(obj.Address())
		require.NoError(t, err)

		want := map[string]uint64{
			phyObj:  1,
			rootObj: 1,
			tsObj:   0,
			lockObj: 0,
			linkObj: 0,
			gcObj:   1,
		}
		check(t, mm, want, 0)
		checkRestarted(t, dir, sh, want, 0)
	})
}

func shardWithMetrics(t *testing.T, path string) (*shard.Shard, *metricsStore) {
	mm := &metricsStore{
		objectCounters: map[string]uint64{
			"phy":  0,
			"root": 0,
			"ts":   0,
			"lock": 0,
			"link": 0,
			"gc":   0,
		},
		containerSize: make(map[cid.ID]int64),
	}

	sh := shard.New(
		shard.WithBlobstor(fstree.New(
			fstree.WithPath(filepath.Join(path, "fstree")),
			fstree.WithDepth(1)),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(path, "meta")),
			meta.WithEpochState(epochState{}),
			meta.WithMaxBatchDelay(time.Microsecond)),
		shard.WithMetricsWriter(mm),
	)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	t.Cleanup(func() {
		sh.Close()
	})

	return sh, mm
}
