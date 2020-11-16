package meta

import (
	"crypto/rand"
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/stretchr/testify/require"
)

func addNFilters(fs *objectSDK.SearchFilters, n int) {
	for i := 0; i < n; i++ {
		key := make([]byte, 32)
		rand.Read(key)

		val := make([]byte, 32)
		rand.Read(val)

		fs.AddFilter(string(key), string(val), objectSDK.MatchStringEqual)
	}
}

func BenchmarkDB_Select(b *testing.B) {
	db := newDB(b)

	defer releaseDB(db)

	for i := 0; i < 100; i++ {
		obj := generateObject(b, testPrm{
			withParent: true,
			attrNum:    100,
		})

		require.NoError(b, db.Put(obj))
	}

	for _, item := range []struct {
		name    string
		filters func(*objectSDK.SearchFilters)
	}{
		{
			name: "empty",
			filters: func(*objectSDK.SearchFilters) {
				return
			},
		},
		{
			name: "1 filter",
			filters: func(fs *objectSDK.SearchFilters) {
				addNFilters(fs, 1)
			},
		},
		{
			name: "10 filters",
			filters: func(fs *objectSDK.SearchFilters) {
				addNFilters(fs, 10)
			},
		},
		{
			name: "100 filters",
			filters: func(fs *objectSDK.SearchFilters) {
				addNFilters(fs, 100)
			},
		},
		{
			name: "1000 filters",
			filters: func(fs *objectSDK.SearchFilters) {
				addNFilters(fs, 1000)
			},
		},
	} {
		b.Run(item.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				fs := new(objectSDK.SearchFilters)
				item.filters(fs)
				b.StartTimer()

				_, err := db.Select(*fs)

				b.StopTimer()
				require.NoError(b, err)
				b.StartTimer()
			}
		})
	}
}

func TestMismatchAfterMatch(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	obj := generateObject(t, testPrm{
		attrNum: 1,
	})

	require.NoError(t, db.Put(obj))

	a := obj.Attributes()[0]

	fs := objectSDK.SearchFilters{}

	// 1st - mismatching filter
	fs.AddFilter(a.Key(), a.Value()+"1", objectSDK.MatchStringEqual)

	// 2nd - matching filter
	fs.AddFilter(a.Key(), a.Value(), objectSDK.MatchStringEqual)

	testSelect(t, db, fs)
}

func addCommonAttribute(objs ...*object.Object) *objectSDK.Attribute {
	aCommon := objectSDK.NewAttribute()
	aCommon.SetKey("common key")
	aCommon.SetValue("common value")

	for _, o := range objs {
		object.NewRawFromObject(o).SetAttributes(
			append(o.Attributes(), aCommon)...,
		)
	}

	return aCommon
}

func TestSelectRemoved(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	// create 2 objects
	obj1 := generateObject(t, testPrm{})
	obj2 := generateObject(t, testPrm{})

	// add common attribute
	a := addCommonAttribute(obj1, obj2)

	// add to DB
	require.NoError(t, db.Put(obj1))
	require.NoError(t, db.Put(obj2))

	fs := objectSDK.SearchFilters{}
	fs.AddFilter(a.Key(), a.Value(), objectSDK.MatchStringEqual)

	testSelect(t, db, fs, obj1.Address(), obj2.Address())

	// remote 1st object
	require.NoError(t, db.Delete(obj1.Address()))

	testSelect(t, db, fs, obj2.Address())
}

func TestMissingObjectAttribute(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	// add object w/o attribute
	obj1 := generateObject(t, testPrm{
		attrNum: 1,
	})

	// add object w/o attribute
	obj2 := generateObject(t, testPrm{})

	a1 := obj1.Attributes()[0]

	// add common attribute
	aCommon := addCommonAttribute(obj1, obj2)

	// write to DB
	require.NoError(t, db.Put(obj1))
	require.NoError(t, db.Put(obj2))

	fs := objectSDK.SearchFilters{}

	// 1st filter by common attribute
	fs.AddFilter(aCommon.Key(), aCommon.Value(), objectSDK.MatchStringEqual)

	// next filter by attribute from 1st object only
	fs.AddFilter(a1.Key(), a1.Value(), objectSDK.MatchStringEqual)

	testSelect(t, db, fs, obj1.Address())
}

func TestSelectParentID(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	// generate 2 objects
	obj1 := generateObject(t, testPrm{})
	obj2 := generateObject(t, testPrm{})

	// set parent ID of 1st object
	par := testOID()
	object.NewRawFromObject(obj1).SetParentID(par)

	// store objects
	require.NoError(t, db.Put(obj1))
	require.NoError(t, db.Put(obj2))

	// filter by parent ID
	fs := objectSDK.SearchFilters{}
	fs.AddParentIDFilter(objectSDK.MatchStringEqual, par)

	testSelect(t, db, fs, obj1.Address())
}
