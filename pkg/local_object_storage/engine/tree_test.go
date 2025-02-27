package engine

import (
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

func BenchmarkTreeVsSearch(b *testing.B) {
	b.Run("10 objects", func(b *testing.B) {
		benchmarkTreeVsSearch(b, 10)
	})
	b.Run("100 objects", func(b *testing.B) {
		benchmarkTreeVsSearch(b, 100)
	})
	b.Run("1000 objects", func(b *testing.B) {
		benchmarkTreeVsSearch(b, 1000)
	})
}

func benchmarkTreeVsSearch(b *testing.B, objCount int) {
	e, _, _ := newEngineWithErrorThreshold(b, "", 0)
	cid := cidtest.ID()
	d := pilorama.CIDDescriptor{CID: cid, Position: 0, Size: 1}
	treeID := "someTree"

	for i := range objCount {
		obj := generateObjectWithCID(cid)
		addAttribute(obj, pilorama.AttributeFilename, strconv.Itoa(i))
		err := e.Put(obj, nil, 0)
		if err != nil {
			b.Fatal(err)
		}
		_, err = e.TreeAddByPath(d, treeID, pilorama.AttributeFilename, nil,
			[]pilorama.KeyValue{{Key: pilorama.AttributeFilename, Value: []byte(strconv.Itoa(i))}})
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Run("search", func(b *testing.B) {
		var fs object.SearchFilters
		fs.AddFilter(pilorama.AttributeFilename, strconv.Itoa(objCount/2), object.MatchStringEqual)

		for range b.N {
			res, err := e.Select(cid, fs)
			if err != nil {
				b.Fatal(err)
			}
			if count := len(res); count != 1 {
				b.Fatalf("expected 1 object, got %d", count)
			}
		}
	})
	b.Run("TreeGetByPath", func(b *testing.B) {
		for range b.N {
			nodes, err := e.TreeGetByPath(cid, treeID, pilorama.AttributeFilename, []string{strconv.Itoa(objCount / 2)}, true)
			if err != nil {
				b.Fatal(err)
			}
			if count := len(nodes); count != 1 {
				b.Fatalf("expected 1 object, got %d", count)
			}
		}
	})
}
