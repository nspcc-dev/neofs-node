package meta_test

import (
	"strconv"
	"testing"
	"time"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func BenchmarkSearch(b *testing.B) {
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
		require.NoError(b, db.Put(obj, nil))
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

	b.ResetTimer()
	for range b.N {
		res, _, err := db.Search(cid, sfs, attrs, curs, 1000)
		if err != nil {
			b.Fatal(err)
		}
		if len(res) != 1 {
			b.Fatalf("failed to search")
		}
	}
}
