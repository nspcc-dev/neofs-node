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
	switch {
	case delta > 0:
		m.objectCounters[objectType] += uint64(delta)
	case delta < 0:
		uDelta := uint64(-delta)

		if m.objectCounters[objectType] >= uDelta {
			m.objectCounters[objectType] -= uDelta
		} else {
			m.objectCounters[objectType] = 0
		}
	default:
		return
	}
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
			err := sh.MarkGarbage(oo[i].GetContainerID(), []oid.ID{oo[i].GetID()})
			require.NoError(t, err)
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

			require.EqualValues(t, child.PayloadSize(), mm.containerSize[cnr])
			require.Equal(t, phyBefore, mm.objectCounters[phyObj])
			require.Equal(t, sumPldSizeBefore, mm.payloadSize)

			cnrInfo, err = sh.ContainerInfo(cnr)
			require.NoError(t, err)
			require.EqualValues(t, child.PayloadSize(), cnrInfo.StorageSize)
			require.EqualValues(t, 1, cnrInfo.ObjectsNumber)
		})
	})
}

func TestInhumeContainerCounters(t *testing.T) {
	sh, mm := shardWithMetrics(t, t.TempDir())

	c1 := cidtest.ID()
	c2 := cidtest.ID()

	var objsC1, objsC2 = 5, 7
	var sizeC1, sizeC2 int64 = 0, 0

	for range objsC1 {
		obj := generateObjectWithCID(c1)
		require.NoError(t, sh.Put(obj, nil))
		sizeC1 += int64(obj.PayloadSize())
	}
	for range objsC2 {
		obj := generateObjectWithCID(c2)
		require.NoError(t, sh.Put(obj, nil))
		sizeC2 += int64(obj.PayloadSize())
	}

	total := uint64(objsC1 + objsC2)
	initialPayload := mm.payloadSize
	require.Equal(t, sizeC1+sizeC2, initialPayload)
	require.Equal(t, mm.objectCounters[phyObj], total)

	require.NoError(t, sh.InhumeContainer(c1))

	require.Equal(t, mm.objectCounters[phyObj], total)
	require.Empty(t, mm.containerSize[c1])
	require.Equal(t, mm.containerSize[c2], sizeC2)
	// payload size must remain unchanged after logical removal
	require.Equal(t, initialPayload, mm.payloadSize)

	require.NoError(t, sh.InhumeContainer(c2))

	require.Equal(t, mm.objectCounters[phyObj], total)
	require.Empty(t, mm.containerSize[c1])
	require.Empty(t, mm.containerSize[c2])
	// payload size still unchanged
	require.Equal(t, initialPayload, mm.payloadSize)
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
