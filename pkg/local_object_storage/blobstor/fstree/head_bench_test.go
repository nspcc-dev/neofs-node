package fstree_test

import (
	"testing"
)

func BenchmarkFSTree_HeadVsGet(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			runHeadVsGetBenchmark(b, size, false)
		})
	}
}

func BenchmarkFSTree_HeadVsGet_Compressed(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			runHeadVsGetBenchmark(b, size, true)
		})
	}
}

func runHeadVsGetBenchmark(b *testing.B, payloadSize int, compressed bool) {
	fsTree := setupFSTree(b)

	suffix := ""
	if compressed {
		setupCompressor(b, fsTree)
		suffix = "_Compressed"
	}

	addr := prepareSingleObject(b, fsTree, payloadSize)

	b.Run("Head"+suffix, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := fsTree.Head(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get"+suffix, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_, err := fsTree.Get(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
