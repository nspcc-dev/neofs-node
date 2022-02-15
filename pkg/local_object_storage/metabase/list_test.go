package meta_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/stretchr/testify/require"
)

func TestLisObjectsWithCursor(t *testing.T) {
	db := newDB(t)

	const (
		containers = 5
		total      = containers * 5 // regular + ts + sg + child + lock
	)

	expected := make([]*addressSDK.Address, 0, total)

	// fill metabase with objects
	for i := 0; i < containers; i++ {
		containerID := cidtest.ID()

		// add one regular object
		obj := generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeRegular)
		err := putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, object.AddressOf(obj))

		// add one tombstone
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeTombstone)
		err = putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, object.AddressOf(obj))

		// add one storage group
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeStorageGroup)
		err = putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, object.AddressOf(obj))

		// add one lock
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeLock)
		err = putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, object.AddressOf(obj))

		// add one inhumed (do not include into expected)
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeRegular)
		err = putBig(db, obj)
		require.NoError(t, err)
		ts := generateObjectWithCID(t, containerID)
		err = meta.Inhume(db, object.AddressOf(obj), object.AddressOf(ts))
		require.NoError(t, err)

		// add one child object (do not include parent into expected)
		splitID := objectSDK.NewSplitID()
		parent := generateObjectWithCID(t, containerID)
		addAttribute(parent, "foo", "bar")
		child := generateObjectWithCID(t, containerID)
		child.SetParent(parent)
		child.SetParentID(parent.ID())
		child.SetSplitID(splitID)
		err = putBig(db, child)
		require.NoError(t, err)
		expected = append(expected, object.AddressOf(child))
	}

	expected = sortAddresses(expected)

	t.Run("success with various count", func(t *testing.T) {
		for countPerReq := 1; countPerReq <= total; countPerReq++ {
			got := make([]*addressSDK.Address, 0, total)

			res, cursor, err := meta.ListWithCursor(db, uint32(countPerReq), nil)
			require.NoError(t, err, "count:%d", countPerReq)
			got = append(got, res...)

			expectedIterations := total / countPerReq
			if total%countPerReq == 0 { // remove initial list if aligned
				expectedIterations--
			}

			for i := 0; i < expectedIterations; i++ {
				res, cursor, err = meta.ListWithCursor(db, uint32(countPerReq), cursor)
				require.NoError(t, err, "count:%d", countPerReq)
				got = append(got, res...)
			}

			_, _, err = meta.ListWithCursor(db, uint32(countPerReq), cursor)
			require.ErrorIs(t, err, meta.ErrEndOfListing, "count:%d", countPerReq, cursor)

			got = sortAddresses(got)
			require.Equal(t, expected, got, "count:%d", countPerReq)
		}
	})

	t.Run("invalid count", func(t *testing.T) {
		_, _, err := meta.ListWithCursor(db, 0, nil)
		require.ErrorIs(t, err, meta.ErrEndOfListing)
	})
}

func TestAddObjectDuringListingWithCursor(t *testing.T) {
	db := newDB(t)

	const total = 5

	expected := make(map[string]int, total)

	// fill metabase with objects
	for i := 0; i < total; i++ {
		obj := generateObject(t)
		err := putBig(db, obj)
		require.NoError(t, err)
		expected[object.AddressOf(obj).String()] = 0
	}

	// get half of the objects
	got, cursor, err := meta.ListWithCursor(db, total/2, nil)
	require.NoError(t, err)
	for _, obj := range got {
		if _, ok := expected[obj.String()]; ok {
			expected[obj.String()]++
		}
	}

	// add new objects
	for i := 0; i < total; i++ {
		obj := generateObject(t)
		err = putBig(db, obj)
		require.NoError(t, err)
	}

	// get remaining objects
	for {
		got, cursor, err = meta.ListWithCursor(db, total, cursor)
		if errors.Is(err, meta.ErrEndOfListing) {
			break
		}
		for _, obj := range got {
			if _, ok := expected[obj.String()]; ok {
				expected[obj.String()]++
			}
		}
	}

	// check if all expected objects were fetched after database update
	for _, v := range expected {
		require.Equal(t, 1, v)
	}

}

func sortAddresses(addr []*addressSDK.Address) []*addressSDK.Address {
	sort.Slice(addr, func(i, j int) bool {
		return addr[i].String() < addr[j].String()
	})
	return addr
}
