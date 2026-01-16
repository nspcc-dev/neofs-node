package meta_test

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/nspcc-dev/bbolt"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
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
		addrs  []objectcore.AddressWithAttributes
		cursor *meta.Cursor
		err    error
	)

	b.ReportAllocs()
	for b.Loop() {
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

func BenchmarkDB_ListWithCursor_Attributes(b *testing.B) {
	const attributeNum = 10
	const containerNum = 10
	const objectsPerContainer = 10
	const totalObjects = containerNum * objectsPerContainer

	attrs := make([]string, attributeNum)
	for i := range attrs {
		attrs[i] = "attrs_" + strconv.Itoa(i)
	}

	db := newDB(b)

	for range containerNum {
		cnr := cidtest.ID()
		for range objectsPerContainer {
			obj := generateObjectWithCID(b, cnr)

			as := make([]object.Attribute, len(attrs))
			for i := range attrs {
				as[i] = object.NewAttribute(attrs[i], strconv.Itoa(i))
			}
			obj.SetAttributes(as...)

			require.NoError(b, db.Put(obj))
		}
	}

	benchAttributes := func(b *testing.B, attrs []string) {
		for _, count := range []int{
			1,
			totalObjects / 10,
			totalObjects / 2,
			totalObjects - 1,
			totalObjects,
			totalObjects + 1,
		} {
			b.Run(fmt.Sprintf("total=%d,count=%d", totalObjects, count), func(b *testing.B) {
				for b.Loop() {
					require.NoError(b, traverseListWithCursor(db, count, attrs...))
				}
			})
		}
	}

	b.Run("all hit", func(b *testing.B) {
		benchAttributes(b, attrs)
	})

	b.Run("all miss", func(b *testing.B) {
		other := slices.Clone(attrs)
		for i := range other {
			other[i] += "_"
		}

		benchAttributes(b, other)
	})
}

func TestLisObjectsWithCursor(t *testing.T) {
	db := newDB(t)

	const containers = 5

	var expected []objectcore.AddressWithAttributes

	// fill metabase with objects
	for range containers {
		containerID := cidtest.ID()

		// add one regular object
		obj := generateObjectWithCID(t, containerID)
		obj.SetType(object.TypeRegular)
		err := putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, objectcore.AddressWithAttributes{Address: obj.Address(), Type: object.TypeRegular})

		// add one tombstone
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(object.TypeTombstone)
		err = putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, objectcore.AddressWithAttributes{Address: obj.Address(), Type: object.TypeTombstone})

		// add one lock
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(object.TypeLock)
		err = putBig(db, obj)
		require.NoError(t, err)
		expected = append(expected, objectcore.AddressWithAttributes{Address: obj.Address(), Type: object.TypeLock})

		// add one inhumed (do not include into expected)
		obj = generateObjectWithCID(t, containerID)
		obj.SetType(object.TypeRegular)
		err = putBig(db, obj)
		require.NoError(t, err)
		ts := createTSForObject(containerID, obj.GetID())
		require.NoError(t, db.Put(ts))
		expected = append(expected, objectcore.AddressWithAttributes{Address: ts.Address(), Type: object.TypeTombstone})

		// add one child object (do not include parent into expected)
		splitID := object.NewSplitID()
		parent := generateObjectWithCID(t, containerID)
		addAttribute(parent, "foo", "bar")
		child := generateObjectWithCID(t, containerID)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)
		err = putBig(db, child)
		require.NoError(t, err)
		expected = append(expected, objectcore.AddressWithAttributes{Address: child.Address(), Type: object.TypeRegular})
	}

	expected = sortAddresses(expected)
	total := len(expected)

	t.Run("success with various count", func(t *testing.T) {
		for countPerReq := 1; countPerReq <= total; countPerReq++ {
			got := make([]objectcore.AddressWithAttributes, 0, total)

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

	t.Run("attributes", func(t *testing.T) {
		const containerNum = 10
		const objectsPerContainer = 10
		const totalObjects = containerNum * objectsPerContainer
		const staticAttr, staticVal = "attr_static", "val_static"
		const commonAttr = "attr_common"
		const groupAttr = "attr_group"

		db := newDB(t)

		var exp []objectcore.AddressWithAttributes
		for i := range containerNum {
			cnr := cidtest.ID()
			for j := range objectsPerContainer {
				commonVal := strconv.Itoa(i*objectsPerContainer + j)
				owner := usertest.ID()

				obj := generateObjectWithCID(t, cnr)
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

				require.NoError(t, db.Put(obj))

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
				collected := collectListWithCursor(t, db, count, staticAttr, commonAttr, groupAttr, "$Object:ownerID")
				require.ElementsMatch(t, exp, collected)
			})
		}
	})
}

func TestAddObjectDuringListingWithCursor(t *testing.T) {
	db := newDB(t)

	const total = 5

	expected := make(map[oid.Address]int, total)

	// fill metabase with objects
	for range total {
		obj := generateObject(t)
		err := putBig(db, obj)
		require.NoError(t, err)
		expected[obj.Address()] = 0
	}

	// get half of the objects
	got, cursor, err := metaListWithCursor(db, total/2, nil)
	require.NoError(t, err)
	for _, obj := range got {
		if _, ok := expected[obj.Address]; ok {
			expected[obj.Address]++
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
			if _, ok := expected[obj.Address]; ok {
				expected[obj.Address]++
			}
		}
	}

	// check if all expected objects were fetched after database update
	for _, v := range expected {
		require.Equal(t, 1, v)
	}
}

func sortAddresses(addrWithType []objectcore.AddressWithAttributes) []objectcore.AddressWithAttributes {
	sort.Slice(addrWithType, func(i, j int) bool {
		return addrWithType[i].Address.EncodeToString() < addrWithType[j].Address.EncodeToString()
	})
	return addrWithType
}

func metaListWithCursor(db *meta.DB, count uint32, cursor *meta.Cursor) ([]objectcore.AddressWithAttributes, *meta.Cursor, error) {
	return db.ListWithCursor(int(count), cursor)
}

func collectListWithCursor(t *testing.T, db *meta.DB, count int, attrs ...string) []objectcore.AddressWithAttributes {
	var next, collected []objectcore.AddressWithAttributes
	var crs *meta.Cursor
	var err error
	for {
		next, crs, err = db.ListWithCursor(count, crs, attrs...)
		collected = append(collected, next...)
		if errors.Is(err, meta.ErrEndOfListing) {
			return collected
		}
		require.NoError(t, err)
	}
}

func traverseListWithCursor(db *meta.DB, count int, attrs ...string) error {
	var c *meta.Cursor
	var err error
	for {
		_, c, err = db.ListWithCursor(count, c, attrs...)
		if err != nil {
			if errors.Is(err, meta.ErrEndOfListing) {
				return nil
			}
			return err
		}
	}
}
