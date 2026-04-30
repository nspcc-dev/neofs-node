package meta_test

import (
	"strconv"
	"testing"
	"time"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func BenchmarkSearch(b *testing.B) {
	b.Run("user attribute", func(b *testing.B) {
		const objCount = 1000
		db := newDB(b, meta.WithMaxBatchDelay(time.Microsecond)) // 1000 puts shouldn't delay us much.
		cid := cidtest.ID()

		for i := range objCount {
			var (
				attrfp object.Attribute
				attrts object.Attribute
			)
			attrfp.SetKey(object.AttributeFilePath)
			attrfp.SetValue("path" + strconv.Itoa(i))
			attrts.SetKey(object.AttributeTimestamp)
			attrts.SetValue(strconv.Itoa(1748028502 + i))
			obj := generateObjectWithCID(b, cid)
			obj.SetAttributes(attrfp, attrts)
			require.NoError(b, db.Put(obj))
		}

		fs := object.SearchFilters{}
		fs.AddFilter("FilePath", "path100", object.MatchStringEqual)
		fs.AddFilter("versioning-state", "", object.MatchNotPresent)

		var attrs = []string{object.AttributeFilePath, object.AttributeTimestamp, "versioning-state",
			"delete-marker", "metatype", "objversion", object.AttributeExpirationEpoch, "lock-meta"}

		sfs, curs, err := objectcore.PreprocessSearchQuery(fs, attrs, "")
		if err != nil {
			b.Fatal(err)
		}

		for b.Loop() {
			res, _, err := db.Search(cid, sfs, attrs, curs, 1000)
			if err != nil {
				b.Fatal(err)
			}
			if len(res) != 1 {
				b.Fatalf("failed to search")
			}
		}
	})

	b.Run("associated object with attribute", func(b *testing.B) {
		const objCount = 1000
		db := newDB(b, meta.WithMaxBatchDelay(time.Microsecond))
		cid := cidtest.ID()
		targets := make([]oid.ID, 0, objCount)

		for range objCount {
			target := generateObjectWithCID(b, cid)
			require.NoError(b, db.Put(target))
			targets = append(targets, target.GetID())
		}

		for i := range objCount {
			lock := generateObjectWithCID(b, cid)
			lock.AssociateLocked(targets[i])
			require.NoError(b, db.Put(lock))
		}

		fs := object.SearchFilters{}
		fs.AddFilter(object.AttributeAssociatedObject, targets[objCount/2].EncodeToString(), object.MatchStringEqual)

		sfs, curs, err := objectcore.PreprocessSearchQuery(fs, []string{object.AttributeAssociatedObject}, "")
		if err != nil {
			b.Fatal(err)
		}

		for b.Loop() {
			res, _, err := db.Search(cid, sfs, []string{object.AttributeAssociatedObject}, curs, 1000)
			if err != nil {
				b.Fatal(err)
			}
			if len(res) != 1 {
				b.Fatalf("failed to search")
			}
		}
	})
}

func BenchmarkIsLocked(b *testing.B) {
	db := newDB(b, meta.WithMaxBatchDelay(time.Microsecond))
	cid := cidtest.ID()

	lockedObj := generateObjectWithCID(b, cid)
	require.NoError(b, db.Put(lockedObj))

	lock := generateObjectWithCID(b, cid)
	lock.AssociateLocked(lockedObj.GetID())
	require.NoError(b, db.Put(lock))

	unlockedObj := generateObjectWithCID(b, cid)
	require.NoError(b, db.Put(unlockedObj))

	b.Run("locked", func(b *testing.B) {
		for b.Loop() {
			locked, err := db.IsLocked(lockedObj.Address())
			if err != nil {
				b.Fatal(err)
			}
			if !locked {
				b.Fatal("expected object to be locked")
			}
		}
	})

	b.Run("unlocked", func(b *testing.B) {
		for b.Loop() {
			locked, err := db.IsLocked(unlockedObj.Address())
			if err != nil {
				b.Fatal(err)
			}
			if locked {
				b.Fatal("expected object to be unlocked")
			}
		}
	})
}
