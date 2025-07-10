package meta_test

import (
	"errors"
	"sort"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func BenchmarkListWithCursor(b *testing.B) {
	db := listWithCursorPrepareDB(b)
	b.Run("1 item", func(b *testing.B) {
		benchmarkListWithCursor(b, db, 1)
	})
	b.Run("10 items", func(b *testing.B) {
		benchmarkListWithCursor(b, db, 10)
	})
	b.Run("100 items", func(b *testing.B) {
		benchmarkListWithCursor(b, db, 100)
	})
}

func listWithCursorPrepareDB(b *testing.B) *meta.DB {
	db := newDB(b, meta.WithMaxBatchSize(1), meta.WithBoltDBOptions(&bbolt.Options{
		NoSync: true,
	})) // faster single-thread generation

	obj := generateObject(b)
	for i := range 100_000 { // should be a multiple of all batch sizes
		obj.SetID(oidtest.ID())
		if i%9 == 0 { // let's have 9 objects per container
			obj.SetContainerID(cidtest.ID())
		}
		require.NoError(b, putBig(db, obj))
	}
	return db
}

func benchmarkListWithCursor(b *testing.B, db *meta.DB, batchSize int) {
	var (
		addrs  []object.AddressWithType
		cursor *meta.Cursor
		err    error
	)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_, cursor, err = db.ListWithCursor(batchSize, cursor)
		if err != nil {
			if !errors.Is(err, meta.ErrEndOfListing) {
				b.Fatalf("error: %v", err)
			}
			cursor = nil
		} else if ln := len(addrs); ln != batchSize {
			b.Fatalf("invalid batch size: %d", ln)
		}
	}
}

func TestLisObjectsWithCursor(t *testing.T) {
	db := newDB(t)

	const (
		containers = 5
		total      = containers * 4 // regular + ts + child + lock
	)

	expected := make([]object.AddressWithType, 0, total)

	// fill metabase with objects
	for range containers {
		containerID := cidtest.ID()

		// add one regular object
		obj := generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeRegular)
		err := putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, object.AddressWithType{Address: object.AddressOf(obj), Type: objectSDK.TypeRegular})

		// add one tombstone
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeTombstone)
		err = putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, object.AddressWithType{Address: object.AddressOf(obj), Type: objectSDK.TypeTombstone})

		// add one lock
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeLock)
		err = putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, object.AddressWithType{Address: object.AddressOf(obj), Type: objectSDK.TypeLock})

		// add one inhumed (do not include into expected)
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(objectSDK.TypeRegular)
		err = putBig(db, obj)
		require.NoError(t, err)
		ts := generateObjectWithCID(t, containerID)
		err = metaInhume(db, object.AddressOf(obj), object.AddressOf(ts))
		require.NoError(t, err)

		// add one child object (do not include parent into expected)
		splitID := objectSDK.NewSplitID()
		parent := generateObjectWithCID(t, containerID)
		addAttribute(parent, "foo", "bar")
		child := generateObjectWithCID(t, containerID)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)
		err = putBig(db, child)
		require.NoError(t, err)
		expected = append(expected, object.AddressWithType{Address: object.AddressOf(child), Type: objectSDK.TypeRegular})
	}

	expected = sortAddresses(expected)

	t.Run("success with various count", func(t *testing.T) {
		for countPerReq := 1; countPerReq <= total; countPerReq++ {
			got := make([]object.AddressWithType, 0, total)

			res, cursor, err := metaListWithCursor(db, uint32(countPerReq), nil)
			require.NoError(t, err, "count:%d", countPerReq)
			got = append(got, res...)

			expectedIterations := total / countPerReq
			if total%countPerReq == 0 { // remove initial list if aligned
				expectedIterations--
			}

			for range expectedIterations {
				res, cursor, err = metaListWithCursor(db, uint32(countPerReq), cursor)
				require.NoError(t, err, "count:%d", countPerReq)
				got = append(got, res...)
			}

			_, _, err = metaListWithCursor(db, uint32(countPerReq), cursor)
			require.ErrorIs(t, err, meta.ErrEndOfListing, "count:%d", countPerReq, cursor)

			got = sortAddresses(got)
			require.Equal(t, expected, got, "count:%d", countPerReq)
		}
	})

	t.Run("invalid count", func(t *testing.T) {
		_, _, err := metaListWithCursor(db, 0, nil)
		require.ErrorIs(t, err, meta.ErrEndOfListing)
	})
}

func TestAddObjectDuringListingWithCursor(t *testing.T) {
	db := newDB(t)

	const total = 5

	expected := make(map[string]int, total)

	// fill metabase with objects
	for range total {
		obj := generateObject(t)
		err := putBig(db, obj)
		require.NoError(t, err)
		expected[object.AddressOf(obj).EncodeToString()] = 0
	}

	// get half of the objects
	got, cursor, err := metaListWithCursor(db, total/2, nil)
	require.NoError(t, err)
	for _, obj := range got {
		if _, ok := expected[obj.Address.EncodeToString()]; ok {
			expected[obj.Address.EncodeToString()]++
		}
	}

	// add new objects
	for range total {
		obj := generateObject(t)
		err = putBig(db, obj)
		require.NoError(t, err)
	}

	// get remaining objects
	for {
		got, cursor, err = metaListWithCursor(db, total, cursor)
		if errors.Is(err, meta.ErrEndOfListing) {
			break
		}
		for _, obj := range got {
			if _, ok := expected[obj.Address.EncodeToString()]; ok {
				expected[obj.Address.EncodeToString()]++
			}
		}
	}

	// check if all expected objects were fetched after database update
	for _, v := range expected {
		require.Equal(t, 1, v)
	}
}

func sortAddresses(addrWithType []object.AddressWithType) []object.AddressWithType {
	sort.Slice(addrWithType, func(i, j int) bool {
		return addrWithType[i].Address.EncodeToString() < addrWithType[j].Address.EncodeToString()
	})
	return addrWithType
}

func metaListWithCursor(db *meta.DB, count uint32, cursor *meta.Cursor) ([]object.AddressWithType, *meta.Cursor, error) {
	return db.ListWithCursor(int(count), cursor)
}
