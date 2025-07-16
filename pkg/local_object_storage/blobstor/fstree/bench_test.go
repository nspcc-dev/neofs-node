package fstree_test

import (
	"io"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func BenchmarkFSTree_Head(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			fsTree := fstree.New(fstree.WithPath(b.TempDir()))

			require.NoError(b, fsTree.Open(false))
			require.NoError(b, fsTree.Init())

			testReadOp(b, fsTree, fsTree.Head, "Head", size)
		})
	}
}

func BenchmarkFSTree_Get(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			fsTree := fstree.New(fstree.WithPath(b.TempDir()))

			require.NoError(b, fsTree.Open(false))
			require.NoError(b, fsTree.Init())

			testReadOp(b, fsTree, fsTree.Get, "Get", size)
		})
	}
}

func BenchmarkFSTree_GetStream(b *testing.B) {
	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			fsTree := fstree.New(fstree.WithPath(b.TempDir()))

			require.NoError(b, fsTree.Open(false))
			require.NoError(b, fsTree.Init())

			testGetStreamOp(b, fsTree, size)
		})
	}
}

func testReadOp(b *testing.B, fsTree *fstree.FSTree, read func(address oid.Address) (*objectSDK.Object, error),
	name string, payloadSize int) {
	b.Run(name+"_regular", func(b *testing.B) {
		obj := generateTestObject(payloadSize)
		addr := object.AddressOf(obj)

		require.NoError(b, fsTree.Put(addr, obj.Marshal()))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err := read(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run(name+"_combined", func(b *testing.B) {
		const numObjects = 10

		objMap := make(map[oid.Address][]byte, numObjects)
		addrs := make([]oid.Address, numObjects)
		for i := range numObjects {
			o := generateTestObject(payloadSize)
			objMap[object.AddressOf(o)] = o.Marshal()
			addrs[i] = object.AddressOf(o)
		}
		require.NoError(b, fsTree.PutBatch(objMap))

		b.ReportAllocs()
		b.ResetTimer()
		for k := range b.N {
			_, err := read(addrs[k%numObjects])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run(name+"_compressed", func(b *testing.B) {
		obj := generateTestObject(payloadSize)
		addr := object.AddressOf(obj)

		compressConfig := &compression.Config{
			Enabled: true,
		}
		require.NoError(b, compressConfig.Init())
		fsTree.SetCompressor(compressConfig)
		require.NoError(b, fsTree.Put(addr, obj.Marshal()))

		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err := read(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func testGetStreamOp(b *testing.B, fsTree *fstree.FSTree, payloadSize int) {
	b.Run("GetStream_regular", func(b *testing.B) {
		obj := generateTestObject(payloadSize)
		addr := object.AddressOf(obj)

		require.NoError(b, fsTree.Put(addr, obj.Marshal()))
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
				reader.Close()
			}
		}
	})

	b.Run("GetStream_combined", func(b *testing.B) {
		const numObjects = 10

		objMap := make(map[oid.Address][]byte, numObjects)
		addrs := make([]oid.Address, numObjects)
		for i := range numObjects {
			o := generateTestObject(payloadSize)
			objMap[object.AddressOf(o)] = o.Marshal()
			addrs[i] = object.AddressOf(o)
		}
		require.NoError(b, fsTree.PutBatch(objMap))

		b.ReportAllocs()
		b.ResetTimer()
		for k := range b.N {
			header, reader, err := fsTree.GetStream(addrs[k%numObjects])
			if err != nil {
				b.Fatal(err)
			}
			if header == nil {
				b.Fatal("header is nil")
			}
			if reader != nil {
				reader.Close()
			}
		}
	})

	b.Run("GetStream_compressed", func(b *testing.B) {
		obj := generateTestObject(payloadSize)
		addr := object.AddressOf(obj)

		compressConfig := &compression.Config{
			Enabled: true,
		}
		require.NoError(b, compressConfig.Init())
		fsTree.SetCompressor(compressConfig)
		require.NoError(b, fsTree.Put(addr, obj.Marshal()))

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
				reader.Close()
			}
		}
	})

	b.Run("GetStream_with_payload_read", func(b *testing.B) {
		obj := generateTestObject(payloadSize)
		addr := object.AddressOf(obj)

		require.NoError(b, fsTree.Put(addr, obj.Marshal()))
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
				reader.Close()
			}
		}
	})
}
