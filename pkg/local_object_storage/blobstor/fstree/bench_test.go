package fstree_test

import (
	"io"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func BenchmarkFSTree_Head(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			fsTree := setupFSTree(b)
			runReadBenchmark(b, fsTree, fsTree.Head, "Head", size)
		})
	}
}

func BenchmarkFSTree_Get(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			fsTree := setupFSTree(b)
			runReadBenchmark(b, fsTree, fsTree.Get, "Get", size)
		})
	}
}

func BenchmarkFSTree_GetStream(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			fsTree := setupFSTree(b)
			runGetStreamBenchmark(b, fsTree, size)
		})
	}
}

func runReadBenchmark(b *testing.B, fsTree *fstree.FSTree, readFunc func(oid.Address) (*objectSDK.Object, error), name string, payloadSize int) {
	b.Run(name+"_regular", func(b *testing.B) {
		addr := prepareSingleObject(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err := readFunc(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run(name+"_combined", func(b *testing.B) {
		addrs := prepareMultipleObjects(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for k := range b.N {
			_, err := readFunc(addrs[k%len(addrs)])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run(name+"_compressed", func(b *testing.B) {
		setupCompressor(b, fsTree)
		addr := prepareSingleObject(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err := readFunc(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func runGetStreamBenchmark(b *testing.B, fsTree *fstree.FSTree, payloadSize int) {
	b.Run("GetStream_regular", func(b *testing.B) {
		addr := prepareSingleObject(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			header, reader, err := fsTree.GetStream(addr)
			if err != nil {
				b.Fatal(err)
			}
			if header == nil {
				b.Fatal("header is nil")
			}
			if reader != nil {
				require.NoError(b, reader.Close())
			}
		}
	})

	b.Run("GetStream_combined", func(b *testing.B) {
		addrs := prepareMultipleObjects(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for k := range b.N {
			header, reader, err := fsTree.GetStream(addrs[k%len(addrs)])
			if err != nil {
				b.Fatal(err)
			}
			if header == nil {
				b.Fatal("header is nil")
			}
			if reader != nil {
				require.NoError(b, reader.Close())
			}
		}
	})

	b.Run("GetStream_compressed", func(b *testing.B) {
		setupCompressor(b, fsTree)
		addr := prepareSingleObject(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			header, reader, err := fsTree.GetStream(addr)
			if err != nil {
				b.Fatal(err)
			}
			if header == nil {
				b.Fatal("header is nil")
			}
			if reader != nil {
				require.NoError(b, reader.Close())
			}
		}
	})

	b.Run("GetStream_with_payload_read", func(b *testing.B) {
		addr := prepareSingleObject(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			header, reader, err := fsTree.GetStream(addr)
			if err != nil {
				b.Fatal(err)
			}
			if header == nil {
				b.Fatal("header is nil")
			}
			if reader != nil {
				// Read all payload to simulate real usage
				_, err := io.ReadAll(reader)
				if err != nil {
					b.Fatal(err)
				}
				require.NoError(b, reader.Close())
			}
		}
	})
}
