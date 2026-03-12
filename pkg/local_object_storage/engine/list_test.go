package engine

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestListWithCursor(t *testing.T) {
	s1 := testNewShard(t, 1)
	s2 := testNewShard(t, 2)
	e := testNewEngineWithShards(s1, s2)

	t.Cleanup(func() {
		e.Close()
	})

	const total = 20

	expected := make([]objectcore.AddressWithAttributes, 0, total)
	got := make([]objectcore.AddressWithAttributes, 0, total)

	for range total {
		containerID := cidtest.ID()
		obj := generateObjectWithCID(containerID)

		err := e.Put(obj, nil)
		require.NoError(t, err)
		expected = append(expected, objectcore.AddressWithAttributes{Type: object.TypeRegular, Address: obj.Address()})
	}

	slices.SortFunc(expected, func(a, b objectcore.AddressWithAttributes) int {
		return a.Address.Compare(b.Address)
	})

	addrs, cursor, err := e.ListWithCursor(1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, addrs)
	got = append(got, addrs...)

	for range total - 1 {
		addrs, cursor, err = e.ListWithCursor(1, cursor)
		if errors.Is(err, ErrEndOfListing) {
			break
		}
		got = append(got, addrs...)
	}

	_, _, err = e.ListWithCursor(1, cursor)
	require.ErrorIs(t, err, ErrEndOfListing)

	got = stripShardIDs(got)
	require.Equal(t, expected, got)

	t.Run("attributes", func(t *testing.T) {
		const containerNum = 10
		const objectsPerContainer = 10
		const totalObjects = containerNum * objectsPerContainer
		const staticAttr, staticVal = "attr_static", "val_static"
		const commonAttr = "attr_common"
		const groupAttr = "attr_group"

		var exp []objectcore.AddressWithAttributes
		var objs []object.Object
		for i := range containerNum {
			cnr := cidtest.ID()
			for j := range objectsPerContainer {
				commonVal := strconv.Itoa(i*objectsPerContainer + j)
				owner := usertest.ID()

				obj := generateObjectWithCID(cnr)
				obj.SetOwner(owner)
				obj.SetType(object.TypeRegular)
				obj.SetAttributes(
					object.NewAttribute(staticAttr, staticVal),
					object.NewAttribute(commonAttr, commonVal),
				)

				var groupVal string
				if j == 0 {
					groupVal = strconv.Itoa(i)
					addAttribute(obj, groupAttr, groupVal)
				}

				objs = append(objs, *obj)
				exp = append(exp, objectcore.AddressWithAttributes{
					Address:    obj.Address(),
					Type:       object.TypeRegular,
					Attributes: []string{staticVal, commonVal, groupVal, string(owner[:])},
				})
			}
		}

		for _, shardNum := range []int{1, 5, 10} {
			t.Run(fmt.Sprintf("shard=%d", shardNum), func(t *testing.T) {
				s := testNewEngineWithShardNum(t, shardNum)
				for i := range objs {
					require.NoError(t, s.Put(&objs[i], nil))
				}

				for _, count := range []uint32{
					1,
					totalObjects / 10,
					totalObjects / 2,
					totalObjects - 1,
					totalObjects,
					totalObjects + 1,
				} {
					t.Run(fmt.Sprintf("total=%d,count=%d", totalObjects, count), func(t *testing.T) {
						collected := collectListWithCursor(t, s, count, staticAttr, commonAttr, groupAttr, "$Object:ownerID")
						require.ElementsMatch(t, exp, stripShardIDs(collected))
					})
				}
			})
		}
	})
}

func TestListWithCursor_Dedup(t *testing.T) {
	s1 := testNewShard(t, 1)
	s2 := testNewShard(t, 2)
	e := testNewEngineWithShards(s1, s2)
	t.Cleanup(func() { _ = e.Close() })

	obj := generateObjectWithCID(cidtest.ID())
	require.NoError(t, s1.Put(obj, nil))
	require.NoError(t, s2.Put(obj, nil))

	res, cursor, err := e.ListWithCursor(2, nil)
	require.NoError(t, err)

	_, _, err = e.ListWithCursor(2, cursor)
	require.ErrorIs(t, err, ErrEndOfListing)

	require.Len(t, res, 1)
	require.Equal(t, obj.Address(), res[0].Address)
	require.Len(t, res[0].ShardIDs, 2)
}

func stripShardIDs(addrs []objectcore.AddressWithAttributes) []objectcore.AddressWithAttributes {
	res := make([]objectcore.AddressWithAttributes, len(addrs))
	for i, a := range addrs {
		a.ShardIDs = nil
		res[i] = a
	}
	return res
}

func collectListWithCursor(t *testing.T, s *StorageEngine, count uint32, attrs ...string) []objectcore.AddressWithAttributes {
	var next, collected []objectcore.AddressWithAttributes
	var crs *Cursor
	var err error
	for {
		next, crs, err = s.ListWithCursor(count, crs, attrs...)
		collected = append(collected, next...)
		if errors.Is(err, ErrEndOfListing) {
			return collected
		}
		require.NoError(t, err)
	}
}

func TestMergeListResults(t *testing.T) {
	var cnr = cid.ID{0xff}

	mkItems := func(ids ...oid.ID) []objectcore.AddressWithAttributes {
		items := make([]objectcore.AddressWithAttributes, len(ids))
		for i, id := range ids {
			items[i].Address = oid.NewAddress(cnr, id)
		}
		return items
	}

	type shardInput struct {
		id    string
		items []objectcore.AddressWithAttributes
	}

	mergeAll := func(count int, shards ...shardInput) []objectcore.AddressWithAttributes {
		var result []objectcore.AddressWithAttributes
		for _, s := range shards {
			result = mergeListResults(nil, result, s.items, s.id, count)
		}
		return result
	}

	t.Run("nil input", func(t *testing.T) {
		require.Empty(t, mergeListResults(nil, nil, nil, "A", 10))
	})
	t.Run("empty shards", func(t *testing.T) {
		require.Empty(t, mergeAll(10,
			shardInput{"A", nil},
			shardInput{"B", nil},
			shardInput{"C", nil},
		))
	})
	t.Run("single shard", func(t *testing.T) {
		res := mergeListResults(nil, nil, mkItems(oid.ID{1}, oid.ID{2}, oid.ID{3}), "A", 10)
		require.Len(t, res, 3)
		for i, r := range res {
			require.Equal(t, oid.NewAddress(cnr, oid.ID{byte(i + 1)}), r.Address)
			require.Equal(t, []string{"A"}, r.ShardIDs)
		}
	})
	t.Run("two shards no duplicates", func(t *testing.T) {
		res := mergeListResults(nil, nil, mkItems(oid.ID{1}, oid.ID{3}, oid.ID{5}), "A", 10)
		res = mergeListResults(nil, res, mkItems(oid.ID{2}, oid.ID{4}, oid.ID{6}), "B", 10)
		require.Len(t, res, 6)
		for i, r := range res {
			require.Equal(t, oid.NewAddress(cnr, oid.ID{byte(i + 1)}), r.Address)
		}
		require.Equal(t, []string{"A"}, res[0].ShardIDs)
		require.Equal(t, []string{"B"}, res[1].ShardIDs)
	})
	t.Run("two shards with duplicates", func(t *testing.T) {
		res := mergeListResults(nil, nil, mkItems(oid.ID{1}, oid.ID{2}, oid.ID{3}), "A", 10)
		res = mergeListResults(nil, res, mkItems(oid.ID{2}, oid.ID{3}, oid.ID{4}), "B", 10)
		require.Len(t, res, 4)
		require.Equal(t, []string{"A"}, res[0].ShardIDs)              // oid{1}
		require.ElementsMatch(t, []string{"A", "B"}, res[1].ShardIDs) // oid{2}
		require.ElementsMatch(t, []string{"A", "B"}, res[2].ShardIDs) // oid{3}
		require.Equal(t, []string{"B"}, res[3].ShardIDs)              // oid{4}
	})
	t.Run("four shards with duplicates", func(t *testing.T) {
		res := mergeAll(10,
			shardInput{"A", mkItems(oid.ID{1}, oid.ID{2}, oid.ID{3})},
			shardInput{"B", mkItems(oid.ID{2}, oid.ID{3}, oid.ID{4}, oid.ID{5})},
			shardInput{"C", mkItems(oid.ID{3}, oid.ID{4}, oid.ID{5})},
			shardInput{"D", mkItems(oid.ID{1}, oid.ID{4}, oid.ID{6}, oid.ID{7})},
		)
		require.Len(t, res, 7)
		require.ElementsMatch(t, []string{"A", "D"}, res[0].ShardIDs)      // oid{1}
		require.ElementsMatch(t, []string{"A", "B"}, res[1].ShardIDs)      // oid{2}
		require.ElementsMatch(t, []string{"A", "B", "C"}, res[2].ShardIDs) // oid{3}
		require.ElementsMatch(t, []string{"B", "C", "D"}, res[3].ShardIDs) // oid{4}
		require.ElementsMatch(t, []string{"B", "C"}, res[4].ShardIDs)      // oid{5}
	})
	t.Run("count limits result", func(t *testing.T) {
		res := mergeListResults(nil, nil, mkItems(oid.ID{1}, oid.ID{2}, oid.ID{3}), "A", 3)
		res = mergeListResults(nil, res, mkItems(oid.ID{1}, oid.ID{4}, oid.ID{5}), "B", 3)
		require.Len(t, res, 3)
		require.Equal(t, oid.NewAddress(cnr, oid.ID{1}), res[0].Address)
		require.Equal(t, oid.NewAddress(cnr, oid.ID{2}), res[1].Address)
		require.Equal(t, oid.NewAddress(cnr, oid.ID{3}), res[2].Address)
	})
	t.Run("count zero", func(t *testing.T) {
		require.Empty(t, mergeListResults(nil, nil, mkItems(oid.ID{1}, oid.ID{2}), "A", 0))
	})
}
