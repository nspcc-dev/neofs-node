package fstree_test

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

var payloadSizes = []int{
	0,           // Empty payload
	100,         // 100 bytes
	4 * 1024,    // 4 KB
	16 * 1024,   // 16 KB
	32 * 1024,   // 32 KB
	100 * 1024,  // 100 KB
	1024 * 1024, // 1 MB
}

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

func runHeadVsGetBenchmark(b *testing.B, payloadSize int, compressed bool) {
	fsTree := fstree.New(fstree.WithPath(b.TempDir()))

	suffix := ""
	if compressed {
		compressConfig := &compression.Config{
			Enabled: true,
		}
		require.NoError(b, compressConfig.Init())
		fsTree.SetCompressor(compressConfig)
		suffix = "_Compressed"
	}

	require.NoError(b, fsTree.Open(false))
	require.NoError(b, fsTree.Init())

	obj := generateTestObject(payloadSize)
	addr := object.AddressOf(obj)

	require.NoError(b, fsTree.Put(addr, obj.Marshal()))

	b.Run("Head"+suffix, func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			_, err := fsTree.Head(addr)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Get"+suffix, func(b *testing.B) {
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
	} else {
		obj.SetPayload(nil)
	}

	return &obj
}
