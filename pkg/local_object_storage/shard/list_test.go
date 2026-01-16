package shard_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestShard_List(t *testing.T) {
	sh := newShard(t, false)
	shWC := newShard(t, true)

	defer func() {
		releaseShard(sh, t)
		releaseShard(shWC, t)
	}()

	t.Run("without write cache", func(t *testing.T) {
		testShardList(t, sh)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardList(t, shWC)
	})
}

func testShardList(t *testing.T, sh *shard.Shard) {
	const C = 10
	const N = 5

	objs := make(map[oid.Address]int)

	for range C {
		cnr := cidtest.ID()

		for range N {
			obj := generateObjectWithCID(cnr)
			addPayload(obj, 1<<2)

			// add parent as virtual object, it must be ignored in List()
			parent := generateObjectWithCID(cnr)
			idParent := parent.GetID()
			obj.SetParentID(idParent)
			obj.SetParent(parent)

			objs[obj.Address()] = 0

			err := sh.Put(obj, nil)
			require.NoError(t, err)
		}
	}

	res, err := sh.List()
	require.NoError(t, err)

	for _, objID := range res {
		i, ok := objs[objID]
		require.True(t, ok)
		require.Equal(t, 0, i)

		objs[objID] = 1
	}
}

func TestShard_ListWithCursor(t *testing.T) {
	t.Run("attributes", func(t *testing.T) {
		const containerNum = 10
		const objectsPerContainer = 10
		const totalObjects = containerNum * objectsPerContainer
		const staticAttr, staticVal = "attr_static", "val_static"
		const commonAttr = "attr_common"
		const groupAttr = "attr_group"

		s := newShard(t, true)
		defer releaseShard(s, t)

		var exp []objectcore.AddressWithAttributes
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

				require.NoError(t, s.Put(obj, nil))

				exp = append(exp, objectcore.AddressWithAttributes{
					Address:    obj.Address(),
					Type:       object.TypeRegular,
					Attributes: []string{staticVal, commonVal, groupVal, string(owner[:])},
				})
			}
		}

		for _, count := range []int{
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

func collectListWithCursor(t *testing.T, s *shard.Shard, count int, attrs ...string) []objectcore.AddressWithAttributes {
	var next, collected []objectcore.AddressWithAttributes
	var crs *shard.Cursor
	var err error
	for {
		next, crs, err = s.ListWithCursor(count, crs, attrs...)
		collected = append(collected, next...)
		if errors.Is(err, shard.ErrEndOfListing) {
			return collected
		}
		require.NoError(t, err)
	}
}
