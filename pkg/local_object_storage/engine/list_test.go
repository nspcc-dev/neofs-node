package engine

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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

	expected := make([]object.AddressWithAttributes, 0, total)
	got := make([]object.AddressWithAttributes, 0, total)

	for range total {
		containerID := cidtest.ID()
		obj := generateObjectWithCID(containerID)

		err := e.Put(obj, nil)
		require.NoError(t, err)
		expected = append(expected, object.AddressWithAttributes{Type: objectSDK.TypeRegular, Address: object.AddressOf(obj)})
	}

	expected = sortAddresses(expected)

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

	got = sortAddresses(got)
	require.Equal(t, expected, got)

	t.Run("attributes", func(t *testing.T) {
		const containerNum = 10
		const objectsPerContainer = 10
		const totalObjects = containerNum * objectsPerContainer
		const staticAttr, staticVal = "attr_static", "val_static"
		const commonAttr = "attr_common"
		const groupAttr = "attr_group"

		var exp []object.AddressWithAttributes
		var objs []objectSDK.Object
		for i := range containerNum {
			cnr := cidtest.ID()
			for j := range objectsPerContainer {
				commonVal := strconv.Itoa(i*objectsPerContainer + j)
				owner := usertest.ID()

				obj := generateObjectWithCID(cnr)
				obj.SetOwner(owner)
				obj.SetType(objectSDK.TypeRegular)
				obj.SetAttributes(
					objectSDK.NewAttribute(staticAttr, staticVal),
					objectSDK.NewAttribute(commonAttr, commonVal),
				)

				var groupVal string
				if j == 0 {
					groupVal = strconv.Itoa(i)
					addAttribute(obj, groupAttr, groupVal)
				}

				objs = append(objs, *obj)
				exp = append(exp, object.AddressWithAttributes{
					Address:    object.AddressOf(obj),
					Type:       objectSDK.TypeRegular,
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
						require.ElementsMatch(t, exp, collected)
					})
				}
			})
		}
	})
}

func sortAddresses(addrWithType []object.AddressWithAttributes) []object.AddressWithAttributes {
	sort.Slice(addrWithType, func(i, j int) bool {
		return addrWithType[i].Address.EncodeToString() < addrWithType[j].Address.EncodeToString()
	})
	return addrWithType
}

func collectListWithCursor(t *testing.T, s *StorageEngine, count uint32, attrs ...string) []object.AddressWithAttributes {
	var next, collected []object.AddressWithAttributes
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
