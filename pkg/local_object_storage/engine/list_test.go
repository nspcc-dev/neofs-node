package engine

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestListWithCursor(t *testing.T) {
	const shardNum = 2
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

	expected = sortAddresses(expected)

	addrs, cursor, err := e.ListWithCursor(1, nil)
	require.NoError(t, err)
	require.Len(t, addrs, shardNum)
	got = append(got, addrs...)

	for {
		addrs, cursor, err = e.ListWithCursor(1, cursor)
		if errors.Is(err, ErrEndOfListing) {
			break
		}
		got = append(got, addrs...)
	}

	got = sortAddresses(stripShardIDs(got))
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

	var all []objectcore.AddressWithAttributes
	var cursor *Cursor
	var err error
	for {
		var batch []objectcore.AddressWithAttributes
		batch, cursor, err = e.ListWithCursor(2, cursor)
		all = append(all, batch...)
		if errors.Is(err, ErrEndOfListing) {
			break
		}
		require.NoError(t, err)
	}

	require.Len(t, all, 1)
	require.Equal(t, obj.Address(), all[0].Address)
	require.Len(t, all[0].ShardIDs, 2)
}

func sortAddresses(addrWithType []objectcore.AddressWithAttributes) []objectcore.AddressWithAttributes {
	sort.Slice(addrWithType, func(i, j int) bool {
		return addrWithType[i].Address.EncodeToString() < addrWithType[j].Address.EncodeToString()
	})
	return addrWithType
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
