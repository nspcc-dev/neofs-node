package object

import (
	"math/big"
	"math/rand/v2"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestMetaInfo(t *testing.T) {
	oID := oidtest.ID()
	cID := cidtest.ID()
	size := rand.Uint64()
	deleted := oidtest.IDs(10)
	locked := oidtest.IDs(10)
	validUntil := rand.Uint64()

	raw := EncodeReplicationMetaInfo(cID, oID, size, deleted, locked, validUntil)
	item, err := stackitem.Deserialize(raw)
	require.NoError(t, err)

	require.Equal(t, stackitem.MapT, item.Type())
	mm, ok := item.Value().([]stackitem.MapElement)
	require.True(t, ok)

	require.Len(t, mm, currentVersion)

	require.Equal(t, cidKey, string(mm[0].Key.Value().([]byte)))
	require.Equal(t, cID[:], mm[0].Value.Value().([]byte))

	require.Equal(t, oidKey, string(mm[1].Key.Value().([]byte)))
	require.Equal(t, oID[:], mm[1].Value.Value().([]byte))

	require.Equal(t, sizeKey, string(mm[2].Key.Value().([]byte)))
	require.Equal(t, size, mm[2].Value.Value().(*big.Int).Uint64())

	require.Equal(t, deletedKey, string(mm[3].Key.Value().([]byte)))
	require.Equal(t, deleted, stackItemToOIDs(t, mm[3].Value))

	require.Equal(t, lockedKey, string(mm[4].Key.Value().([]byte)))
	require.Equal(t, locked, stackItemToOIDs(t, mm[4].Value))

	require.Equal(t, validUntilKey, string(mm[5].Key.Value().([]byte)))
	require.Equal(t, validUntil+validInterval, mm[5].Value.Value().(*big.Int).Uint64())
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
