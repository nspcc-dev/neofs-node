package fstree_test

import (
	"testing"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
)

func BenchmarkFSTree_HeadVsGet(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			runHeadVsGetBenchmark(b, size)
		})
	}
}

func runHeadVsGetBenchmark(b *testing.B, payloadSize int) {
	fsTree := setupFSTree(b)

	addr := prepareSingleObject(b, fsTree, payloadSize)

	b.Run("Head", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := fsTree.Head(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("ReadHeader", func(b *testing.B) {
		buf := make([]byte, iobject.NonPayloadFieldsBufferLength*2)

		b.ReportAllocs()
		for b.Loop() {
			_, err := fsTree.ReadHeader(addr, buf)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := fsTree.Get(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
