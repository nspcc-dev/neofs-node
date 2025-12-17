package shard_test

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/bbolt"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

		require.Equal(t, uint64(objNumber), mm.objectCounters[physical])
		require.Equal(t, uint64(objNumber), mm.objectCounters[logical])
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload, mm.payloadSize)
	})

	t.Run("inhume_GC", func(t *testing.T) {
		inhumedNumber := objNumber / 4

		for i := range inhumedNumber {
			err := sh.MarkGarbage(objectcore.AddressOf(oo[i]))
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

			expectedSizes[oo[i].GetContainerID()] -= int64(removedPayload)
		}
		require.Equal(t, expectedSizes, mm.containerSize)
		require.Equal(t, totalPayload-int64(totalRemovedpayload), mm.payloadSize)
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
	require.Equal(t, mm.objectCounters[physical], total)
	require.Equal(t, mm.objectCounters[logical], total)

	require.NoError(t, sh.InhumeContainer(c1))

	require.Equal(t, mm.objectCounters[physical], total)
	require.Equal(t, mm.objectCounters[logical], uint64(objsC2))
	require.Empty(t, mm.containerSize[c1])
	require.Equal(t, mm.containerSize[c2], sizeC2)
	// payload size must remain unchanged after logical removal
	require.Equal(t, initialPayload, mm.payloadSize)

	require.NoError(t, sh.InhumeContainer(c2))

	require.Equal(t, mm.objectCounters[physical], total)
	require.Empty(t, mm.objectCounters[logical])
	require.Empty(t, mm.containerSize[c1])
	require.Empty(t, mm.containerSize[c2])
	// payload size still unchanged
	require.Equal(t, initialPayload, mm.payloadSize)
}

func TestShardCounterMigration(t *testing.T) {
	path := t.TempDir()
	sh, mm := shardWithMetrics(t, path)

	const n = 11
	for range n {
		obj := generateObject()
		require.NoError(t, sh.Put(obj, nil))
	}
	require.Equal(t, uint64(n), mm.objectCounters[physical])
	require.Equal(t, uint64(n), mm.objectCounters[logical])

	// Close shard so we can mutate Bolt file.
	require.NoError(t, sh.Close())

	metaPath := filepath.Join(path, "meta")
	mdb, err := bbolt.Open(metaPath, 0o600, nil)
	require.NoError(t, err)

	var (
		bucketPrefix          = []byte{5} // shardInfoPrefix
		objectPhyCounterKey   = []byte("phy_counter")
		objectLogicCounterKey = []byte("logic_counter")
		corruptPhysical       = uint64(100)
		corruptLogical        = uint64(200)
	)

	require.NoError(t, mdb.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketPrefix)
		require.NotNil(t, b)

		require.Equal(t, uint64(n), binary.LittleEndian.Uint64(b.Get(objectPhyCounterKey)))
		require.Equal(t, uint64(n), binary.LittleEndian.Uint64(b.Get(objectLogicCounterKey)))

		cc := make([]byte, 8)
		binary.LittleEndian.PutUint64(cc, corruptPhysical)
		require.NoError(t, b.Put(objectPhyCounterKey, cc))
		cc = make([]byte, 8)
		binary.LittleEndian.PutUint64(cc, corruptLogical)
		require.NoError(t, b.Put(objectLogicCounterKey, cc))

		// force metabase version to 7 to restore counters
		bkt := tx.Bucket([]byte{0x05})
		require.NotNil(t, bkt)
		require.NoError(t, bkt.Put([]byte("version"), []byte{0x07, 0, 0, 0, 0, 0, 0, 0}))
		return nil
	}))
	require.NoError(t, mdb.Close())

	// re-open shard, initMetrics should force sync counters and restore real values
	sh, mm = shardWithMetrics(t, path)

	require.Equal(t, uint64(n), mm.objectCounters[physical])
	require.Equal(t, uint64(n), mm.objectCounters[logical])
	require.NoError(t, sh.Close())
}

func shardWithMetrics(t *testing.T, path string) (*shard.Shard, *metricsStore) {
	mm := &metricsStore{
		objectCounters: map[string]uint64{
			"phy":   0,
			"logic": 0,
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
