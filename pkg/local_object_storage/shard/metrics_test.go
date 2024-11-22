package shard_test

import (
	"path/filepath"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

type metricsStore struct {
	objectCounters map[string]uint64
	containerSize  map[string]int64
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
	m.containerSize[cnr] += size
}

func (m *metricsStore) AddToPayloadSize(size int64) {
	m.payloadSize += size
}

const physical = "phy"
const logical = "logic"

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
		require.Zero(t, mm.objectCounters[physical])
		require.Zero(t, mm.objectCounters[logical])
		require.Empty(t, mm.containerSize)
		require.Zero(t, mm.payloadSize)
	})

	var totalPayload int64

	expectedSizes := make(map[string]int64)
	for i := range oo {
		cnr := oo[i].GetContainerID()
		oSize := int64(oo[i].PayloadSize())
		expectedSizes[cnr.EncodeToString()] += oSize
		totalPayload += oSize
	}

	t.Run("put", func(t *testing.T) {
		var prm shard.PutPrm

		for i := range objNumber {
			prm.SetObject(oo[i])

			_, err := sh.Put(prm)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(objNumber), mm.objectCounters[physical])
		require.Equal(t, uint64(objNumber), mm.objectCounters[logical])
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload, mm.payloadSize)
	})

	t.Run("inhume_GC", func(t *testing.T) {
		inhumedNumber := objNumber / 4

		for i := range inhumedNumber {
			err := sh.MarkGarbage(false, objectcore.AddressOf(oo[i]))
			require.NoError(t, err)
		}

		require.Equal(t, uint64(objNumber), mm.objectCounters[physical])
		require.Equal(t, uint64(objNumber-inhumedNumber), mm.objectCounters[logical])
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload, mm.payloadSize)

		oo = oo[inhumedNumber:]
	})

	t.Run("inhume_TS", func(t *testing.T) {
		ts := objectcore.AddressOf(generateObject())

		phy := mm.objectCounters[physical]
		logic := mm.objectCounters[logical]

		inhumedNumber := int(phy / 4)

		err := sh.Inhume(ts, 0, addrFromObjs(oo[:inhumedNumber])...)
		require.NoError(t, err)

		require.Equal(t, phy, mm.objectCounters[physical])
		require.Equal(t, logic-uint64(inhumedNumber), mm.objectCounters[logical])
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload, mm.payloadSize)

		oo = oo[inhumedNumber:]
	})

	t.Run("Delete", func(t *testing.T) {
		phy := mm.objectCounters[physical]
		logic := mm.objectCounters[logical]

		deletedNumber := int(phy / 4)

		err := sh.Delete(addrFromObjs(oo[:deletedNumber]))
		require.NoError(t, err)

		require.Equal(t, phy-uint64(deletedNumber), mm.objectCounters[physical])
		require.Equal(t, logic-uint64(deletedNumber), mm.objectCounters[logical])
		var totalRemovedpayload uint64
		for i := range oo[:deletedNumber] {
			removedPayload := oo[i].PayloadSize()
			totalRemovedpayload += removedPayload

			cnr := oo[i].GetContainerID()
			expectedSizes[cnr.EncodeToString()] -= int64(removedPayload)
		}
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload-int64(totalRemovedpayload), mm.payloadSize)
	})
}

func shardWithMetrics(t *testing.T, path string) (*shard.Shard, *metricsStore) {
	blobOpts := []blobstor.Option{
		blobstor.WithStorages([]blobstor.SubStorage{
			{
				Storage: fstree.New(
					fstree.WithDirNameLen(2),
					fstree.WithPath(filepath.Join(path, "blob")),
					fstree.WithDepth(1)),
			},
		}),
	}

	mm := &metricsStore{
		objectCounters: map[string]uint64{
			"phy":   0,
			"logic": 0,
		},
		containerSize: make(map[string]int64),
	}

	sh := shard.New(
		shard.WithBlobStorOptions(blobOpts...),
		shard.WithPiloramaOptions(pilorama.WithPath(filepath.Join(path, "pilorama"))),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(path, "meta")),
			meta.WithEpochState(epochState{})),
		shard.WithMetricsWriter(mm),
	)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	t.Cleanup(func() {
		sh.Close()
	})

	return sh, mm
}

func addrFromObjs(oo []*object.Object) []oid.Address {
	aa := make([]oid.Address, len(oo))

	for i := range oo {
		aa[i] = objectcore.AddressOf(oo[i])
	}

	return aa
}
