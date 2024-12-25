package object

import (
	"math/big"
	"math/rand/v2"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

type m struct {
	cID   cid.ID
	oID   oid.ID
	size  uint64
	vub   uint64
	magic uint32

	first   oid.ID
	prev    oid.ID
	deleted []oid.ID
	locked  []oid.ID
	typ     object.Type
}

func TestMetaInfo(t *testing.T) {
	meta := m{
		cID:     cidtest.ID(),
		oID:     oidtest.ID(),
		size:    rand.Uint64(),
		vub:     rand.Uint64(),
		magic:   rand.Uint32(),
		first:   oidtest.ID(),
		prev:    oidtest.ID(),
		deleted: oidtest.IDs(10),
		locked:  oidtest.IDs(10),
		typ:     object.TypeTombstone,
	}

	t.Run("full", func(t *testing.T) {
		testMeta(t, meta, true)
	})

	t.Run("no optional", func(t *testing.T) {
		meta.first = oid.ID{}
		meta.prev = oid.ID{}
		meta.deleted = nil
		meta.deleted = nil
		meta.locked = nil
		meta.typ = object.TypeRegular

		testMeta(t, meta, false)
	})
}

func testMeta(t *testing.T, m m, full bool) {
	raw := EncodeReplicationMetaInfo(m.cID, m.oID, m.first, m.prev, m.size, m.typ, m.deleted, m.locked, m.vub, m.magic)
	item, err := stackitem.Deserialize(raw)
	require.NoError(t, err)

	require.Equal(t, stackitem.MapT, item.Type())
	mm, ok := item.Value().([]stackitem.MapElement)
	require.True(t, ok)

	require.Equal(t, cidKey, string(mm[0].Key.Value().([]byte)))
	require.Equal(t, m.cID[:], mm[0].Value.Value().([]byte))

	require.Equal(t, oidKey, string(mm[1].Key.Value().([]byte)))
	require.Equal(t, m.oID[:], mm[1].Value.Value().([]byte))

	require.Equal(t, sizeKey, string(mm[2].Key.Value().([]byte)))
	require.Equal(t, m.size, mm[2].Value.Value().(*big.Int).Uint64())

	require.Equal(t, validUntilKey, string(mm[3].Key.Value().([]byte)))
	require.Equal(t, m.vub, mm[3].Value.Value().(*big.Int).Uint64())

	require.Equal(t, networkMagicKey, string(mm[4].Key.Value().([]byte)))
	require.Equal(t, m.magic, uint32(mm[4].Value.Value().(*big.Int).Uint64()))

	if !full {
		require.Len(t, mm, 5)
		return
	}

	require.Equal(t, firstPartKey, string(mm[5].Key.Value().([]byte)))
	require.Equal(t, m.first[:], mm[5].Value.Value().([]byte))

	require.Equal(t, previousPartKey, string(mm[6].Key.Value().([]byte)))
	require.Equal(t, m.prev[:], mm[6].Value.Value().([]byte))

	require.Equal(t, deletedKey, string(mm[7].Key.Value().([]byte)))
	require.Equal(t, m.deleted, stackItemToOIDs(t, mm[7].Value))

	require.Equal(t, lockedKey, string(mm[8].Key.Value().([]byte)))
	require.Equal(t, m.locked, stackItemToOIDs(t, mm[8].Value))

	require.Equal(t, typeKey, string(mm[9].Key.Value().([]byte)))
	require.Equal(t, int(m.typ), int(mm[9].Value.Value().(*big.Int).Uint64()))
}

func stackItemToOIDs(t *testing.T, value stackitem.Item) []oid.ID {
	value, ok := value.(*stackitem.Array)
	require.True(t, ok)

	vv := value.Value().([]stackitem.Item)
	res := make([]oid.ID, 0, len(vv))

	for _, v := range vv {
		raw := v.Value().([]byte)
		res = append(res, oid.ID(raw))
	}

	return res
}
