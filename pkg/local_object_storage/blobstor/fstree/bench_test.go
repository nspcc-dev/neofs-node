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
			runReadBenchmark(b, "Head", size)
		})
	}
}

func BenchmarkFSTree_Get(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			runReadBenchmark(b, "Get", size)
		})
	}
}

func BenchmarkFSTree_GetStream(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			runReadBenchmark(b, "GetStream", size)

			b.Run("GetStream_with_payload_read", func(b *testing.B) {
				freshFSTree := setupFSTree(b)
				addr := prepareSingleObject(b, freshFSTree, size)

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					header, reader, err := freshFSTree.GetStream(addr)
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
		})
	}
}

func runReadBenchmark(b *testing.B, methodName string, payloadSize int) {
	testRead := func(fsTree *fstree.FSTree, addr oid.Address) {
		var err error
		switch methodName {
		case "Head":
			_, err = fsTree.Head(addr)
		case "Get":
			_, err = fsTree.Get(addr)
		case "GetStream":
			var (
				header *objectSDK.Object
				reader io.ReadCloser
			)
			header, reader, err = fsTree.GetStream(addr)
			if header == nil {
				b.Fatal("header is nil")
			}
			if reader != nil {
				require.NoError(b, reader.Close())
			}
		}
		if err != nil {
			b.Fatal(err)
		}
	}

	b.Run(methodName+"_regular", func(b *testing.B) {
		fsTree := setupFSTree(b)
		addr := prepareSingleObject(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			testRead(fsTree, addr)
		}
	})

	b.Run(methodName+"_combined", func(b *testing.B) {
		fsTree := setupFSTree(b)
		addrs := prepareMultipleObjects(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for k := range b.N {
			testRead(fsTree, addrs[k%len(addrs)])
		}
	})

	b.Run(methodName+"_compressed", func(b *testing.B) {
		fsTree := setupFSTree(b)
		setupCompressor(b, fsTree)
		addr := prepareSingleObject(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			testRead(fsTree, addr)
		}
	})

	b.Run(methodName+"_compressed_combined", func(b *testing.B) {
		fsTree := setupFSTree(b)
		setupCompressor(b, fsTree)
		addrs := prepareMultipleObjects(b, fsTree, payloadSize)

		b.ReportAllocs()
		b.ResetTimer()
		for k := range b.N {
			testRead(fsTree, addrs[k%len(addrs)])
		}
	})
}
