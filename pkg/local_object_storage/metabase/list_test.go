package meta_test

import (
	"errors"
	"sort"
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestLisObjectsWithCursor(t *testing.T) {
	db := newDB(t)

	const (
		containers = 5
		total      = containers * 4 // regular + ts + sg + child
	)

	expected := make([]*objectSDK.Address, 0, total)

	// fill metabase with objects
	for i := 0; i < containers; i++ {
		containerID := cidtest.ID()

		// add one regular object
		obj := generateRawObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeRegular)
		err := putBig(db, obj.Object())
		require.NoError(t, err)
		expected = append(expected, obj.Object().Address())

		// add one tombstone
		obj = generateRawObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeTombstone)
		err = putBig(db, obj.Object())
		require.NoError(t, err)
		expected = append(expected, obj.Object().Address())

		// add one storage group
		obj = generateRawObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeStorageGroup)
		err = putBig(db, obj.Object())
		require.NoError(t, err)
		expected = append(expected, obj.Object().Address())

		// add one inhumed (do not include into expected)
		obj = generateRawObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeRegular)
		err = putBig(db, obj.Object())
		require.NoError(t, err)
		ts := generateRawObjectWithCID(t, containerID)
		err = meta.Inhume(db, obj.Object().Address(), ts.Object().Address())
		require.NoError(t, err)

		// add one child object (do not include parent into expected)
		splitID := objectSDK.NewSplitID()
		parent := generateRawObjectWithCID(t, containerID)
		addAttribute(parent, "foo", "bar")
		child := generateRawObjectWithCID(t, containerID)
		child.SetParent(parent.Object().SDK())
		child.SetParentID(parent.ID())
		child.SetSplitID(splitID)
		err = putBig(db, child.Object())
		require.NoError(t, err)
		expected = append(expected, child.Object().Address())
	}

	expected = sortAddresses(expected)

	t.Run("success with various count", func(t *testing.T) {
		for countPerReq := 1; countPerReq <= total; countPerReq++ {
			got := make([]*objectSDK.Address, 0, total)

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
		obj := generateRawObject(t)
		err := putBig(db, obj.Object())
		require.NoError(t, err)
		expected[obj.Object().Address().String()] = 0
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
		obj := generateRawObject(t)
		err = putBig(db, obj.Object())
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

func sortAddresses(addr []*objectSDK.Address) []*objectSDK.Address {
	sort.Slice(addr, func(i, j int) bool {
		return addr[i].String() < addr[j].String()
	})
	return addr
}
