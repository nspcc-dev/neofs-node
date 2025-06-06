package fstree

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
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

func generateTestObject(payloadSize int) *objectSDK.Object {
	var ver version.Version
	ver.SetMajor(2)
	ver.SetMinor(1)

	obj := objectSDK.New()
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetContainerID(cidtest.ID())
	obj.SetVersion(&ver)

	if payloadSize > 0 {
		payload := make([]byte, payloadSize)
		_, _ = rand.Read(payload)
		obj.SetPayload(payload)

		csum := checksum.NewSHA256(sha256.Sum256(payload))

		obj.SetPayloadChecksum(csum)
		obj.SetPayloadHomomorphicHash(checksum.NewTillichZemor(tz.Sum(csum.Value())))
	}

	var attr1, attr2 objectSDK.Attribute
	attr1.SetKey("benchmark")
	attr1.SetValue("test")

	attr2.SetKey("type")
	attr2.SetValue("performance")

	obj.SetAttributes(attr1, attr2)

	return obj
}
