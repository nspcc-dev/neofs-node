package engine

import (
	"os"
	"sort"
	"testing"

	cidtest "github.com/nspcc-dev/neofs-api-go/pkg/container/id/test"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestListWithCursor(t *testing.T) {
	s1 := testNewShard(t, 1)
	s2 := testNewShard(t, 2)
	e := testNewEngineWithShards(s1, s2)

	t.Cleanup(func() {
		e.Close()
		os.RemoveAll(t.Name())
	})

	const total = 20

	expected := make([]*object.Address, 0, total)
	got := make([]*object.Address, 0, total)

	for i := 0; i < total; i++ {
		containerID := cidtest.Generate()
		obj := generateRawObjectWithCID(t, containerID)
		prm := new(PutPrm).WithObject(obj.Object())
		_, err := e.Put(prm)
		require.NoError(t, err)
		expected = append(expected, obj.Object().Address())
	}

	expected = sortAddresses(expected)

	for _, shard := range e.DumpInfo().Shards {
		prm := new(ListWithCursorPrm).WithShardID(*shard.ID).WithCount(total)
		res, err := e.ListWithCursor(prm)
		require.NoError(t, err)
		require.NotEmpty(t, res.AddressList())

		got = append(got, res.AddressList()...)
		_, err = e.ListWithCursor(prm.WithCursor(res.Cursor()))
		require.ErrorIs(t, err, meta.ErrEndOfListing)
	}

	got = sortAddresses(got)
	require.Equal(t, expected, got)
}

func sortAddresses(addr []*object.Address) []*object.Address {
	sort.Slice(addr, func(i, j int) bool {
		return addr[i].String() < addr[j].String()
	})
	return addr
}
