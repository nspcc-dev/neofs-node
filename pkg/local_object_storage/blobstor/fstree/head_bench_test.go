package fstree

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func BenchmarkFSTree_HeadVsGet(b *testing.B) {
	payloadSizes := []int{
		0,           // Empty payload
		100,         // 100 bytes
		10 * 1024,   // 10 KB
		100 * 1024,  // 100 KB
		1024 * 1024, // 1 MB
	}

	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			benchmarkHeadVsGet(b, size)
		})
	}
}

func BenchmarkFSTree_HeadVsGet_Compressed(b *testing.B) {
	payloadSizes := []int{
		100,              // 100 bytes
		10 * 1024,        // 10 KB
		100 * 1024,       // 100 KB
		1024 * 1024,      // 1 MB
		10 * 1024 * 1024, // 10 MB
	}

	for _, size := range payloadSizes {
		b.Run(generateSizeLabel(size), func(b *testing.B) {
			benchmarkHeadVsGetCompressed(b, size)
		})
	}
}

func generateSizeLabel(size int) string {
	switch {
	case size == 0:
		return "Empty"
	case size < 1024:
		return fmt.Sprintf("%dB", size)
	case size < 1024*1024:
		return fmt.Sprintf("%dKB", size/1024)
	default:
		return fmt.Sprintf("%dMB", size/(1024*1024))
	}
}

func benchmarkHeadVsGet(b *testing.B, payloadSize int) {
	fsTree := New(WithPath(b.TempDir()))
	require.NoError(b, fsTree.Open(false))
	require.NoError(b, fsTree.Init())

	obj := generateTestObject(payloadSize)
	addr := object.AddressOf(obj)

	require.NoError(b, fsTree.Put(addr, obj.Marshal()))

	b.Run("Head", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			_, err := fsTree.Head(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			_, err := fsTree.Get(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkHeadVsGetCompressed(b *testing.B, payloadSize int) {
	fsTree := New(WithPath(b.TempDir()))
	compressConfig := &compression.Config{
		Enabled: true,
	}
	require.NoError(b, compressConfig.Init())
	fsTree.SetCompressor(compressConfig)

	require.NoError(b, fsTree.Open(false))
	require.NoError(b, fsTree.Init())

	obj := generateTestObject(payloadSize)
	addr := object.AddressOf(obj)

	require.NoError(b, fsTree.Put(addr, obj.Marshal()))

	b.Run("Head_Compressed", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			_, err := fsTree.Head(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get_Compressed", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			_, err := fsTree.Get(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func generateTestObject(payloadSize int) *objectSDK.Object {
	obj := objecttest.Object()
	if payloadSize > 0 {
		payload := make([]byte, payloadSize)
		_, _ = rand.Read(payload)
		obj.SetPayload(payload)
	}

	return &obj
}
