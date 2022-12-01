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
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

type metricsStore struct {
	objCounters map[string]uint64
	cnrSize     map[string]int64
}

func (m metricsStore) SetShardID(_ string) {}

func (m metricsStore) SetObjectCounter(objectType string, v uint64) {
	m.objCounters[objectType] = v
}

func (m metricsStore) AddToObjectCounter(objectType string, delta int) {
	switch {
	case delta > 0:
		m.objCounters[objectType] += uint64(delta)
	case delta < 0:
		uDelta := uint64(-delta)

		if m.objCounters[objectType] >= uDelta {
			m.objCounters[objectType] -= uDelta
		} else {
			m.objCounters[objectType] = 0
		}
	case delta == 0:
		return
	}
}

func (m metricsStore) IncObjectCounter(objectType string) {
	m.objCounters[objectType] += 1
}

func (m metricsStore) DecObjectCounter(objectType string) {
	m.AddToObjectCounter(objectType, -1)
}

func (m metricsStore) AddToContainerSize(cnr string, size int64) {
	m.cnrSize[cnr] += size
}

const physical = "phy"
const logical = "logic"

func TestCounters(t *testing.T) {
	dir := t.TempDir()
	sh, mm := shardWithMetrics(t, dir)

	const objNumber = 10
	oo := make([]*object.Object, objNumber)
	for i := 0; i < objNumber; i++ {
		oo[i] = generateObject(t)
	}

	t.Run("defaults", func(t *testing.T) {
		require.Zero(t, mm.objCounters[physical])
		require.Zero(t, mm.objCounters[logical])
		require.Empty(t, mm.cnrSize)
	})

	expectedSizes := make(map[string]int64)
	for i := range oo {
		cnr, _ := oo[i].ContainerID()
		expectedSizes[cnr.EncodeToString()] += int64(oo[i].PayloadSize())
	}

	t.Run("put", func(t *testing.T) {
		var prm shard.PutPrm

		for i := 0; i < objNumber; i++ {
			prm.SetObject(oo[i])

			_, err := sh.Put(prm)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(objNumber), mm.objCounters[physical])
		require.Equal(t, uint64(objNumber), mm.objCounters[logical])
		require.Equal(t, expectedSizes, mm.cnrSize)
	})

	t.Run("inhume_GC", func(t *testing.T) {
		var prm shard.InhumePrm
		inhumedNumber := objNumber / 4

		for i := 0; i < inhumedNumber; i++ {
			prm.MarkAsGarbage(objectcore.AddressOf(oo[i]))

			_, err := sh.Inhume(prm)
			require.NoError(t, err)
		}

		require.Equal(t, uint64(objNumber), mm.objCounters[physical])
		require.Equal(t, uint64(objNumber-inhumedNumber), mm.objCounters[logical])
		require.Equal(t, expectedSizes, mm.cnrSize)

		oo = oo[inhumedNumber:]
	})

	t.Run("inhume_TS", func(t *testing.T) {
		var prm shard.InhumePrm
		ts := objectcore.AddressOf(generateObject(t))

		phy := mm.objCounters[physical]
		logic := mm.objCounters[logical]

		inhumedNumber := int(phy / 4)
		prm.SetTarget(ts, addrFromObjs(oo[:inhumedNumber])...)

		_, err := sh.Inhume(prm)
		require.NoError(t, err)

		require.Equal(t, phy, mm.objCounters[physical])
		require.Equal(t, logic-uint64(inhumedNumber), mm.objCounters[logical])
		require.Equal(t, expectedSizes, mm.cnrSize)

		oo = oo[inhumedNumber:]
	})

	t.Run("Delete", func(t *testing.T) {
		var prm shard.DeletePrm

		phy := mm.objCounters[physical]
		logic := mm.objCounters[logical]

		deletedNumber := int(phy / 4)
		prm.SetAddresses(addrFromObjs(oo[:deletedNumber])...)

		_, err := sh.Delete(prm)
		require.NoError(t, err)

		require.Equal(t, phy-uint64(deletedNumber), mm.objCounters[physical])
		require.Equal(t, logic-uint64(deletedNumber), mm.objCounters[logical])
		for i := range oo[:deletedNumber] {
			cnr, _ := oo[i].ContainerID()
			expectedSizes[cnr.EncodeToString()] -= int64(oo[i].PayloadSize())
		}
		require.Equal(t, expectedSizes, mm.cnrSize)
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
		objCounters: map[string]uint64{
			"phy":   0,
			"logic": 0,
		},
		cnrSize: make(map[string]int64),
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

	for i := 0; i < len(oo); i++ {
		aa[i] = objectcore.AddressOf(oo[i])
	}

	return aa
}
