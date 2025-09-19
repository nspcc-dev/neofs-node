package fstree_test

import (
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/stretchr/testify/require"
)

func BenchmarkFSTree_GetRange(b *testing.B) {
	const (
		KB = 1024
		MB = 1024 * 1024
	)

	testCases := []struct {
		from       uint64
		length     uint64
		objectSize int
	}{
		{from: 1 * MB, length: 4 * KB, objectSize: 10 * MB}, // 10MB, range in the middle
		{from: 0, length: 10 * MB, objectSize: 10 * MB},     // 10MB, full range
		{from: 0, length: 0, objectSize: 10 * MB},           // 10MB, zero range
		{from: 1 * KB, length: 4 * KB, objectSize: 1 * MB},  // 1MB, range in the middle
		{from: 0, length: 1 * MB, objectSize: 1 * MB},       // 1MB, full range
		{from: 0, length: 0, objectSize: 1 * MB},            // 1MB, zero range
		{from: 1 * KB, length: 1 * KB, objectSize: 4 * KB},  // 4KB, range in the middle
		{from: 0, length: 4 * KB, objectSize: 4 * KB},       // 4KB, full range
		{from: 0, length: 0, objectSize: 4 * KB},            // 4KB, zero range
	}

	for _, tc := range testCases {
		b.Run(fmt.Sprintf("size=%s,off=%s,len=%s",
			generateSizeLabel(tc.objectSize), generateSizeLabel(int(tc.from)), generateSizeLabel(int(tc.length))),
			func(b *testing.B) {
				obj := generateTestObject(tc.objectSize)
				addr := object.AddressOf(obj)

				b.Run("regular", func(b *testing.B) {
					fsTree := setupFSTree(b)
					require.NoError(b, fsTree.Put(addr, obj.Marshal()))

					for b.Loop() {
						_, err := fsTree.GetRange(addr, tc.from, tc.length)
						if err != nil {
							b.Fatal(err)
						}
					}
				})

				b.Run("compressed", func(b *testing.B) {
					fsTree := setupFSTree(b)
					setupCompressor(b, fsTree)
					require.NoError(b, fsTree.Put(addr, obj.Marshal()))

					for b.Loop() {
						_, err := fsTree.GetRange(addr, tc.from, tc.length)
						if err != nil {
							b.Fatal(err)
						}
					}
				})

				b.Run("combined", func(b *testing.B) {
					fsTree := setupFSTree(b)
					addrs := prepareMultipleObjects(b, fsTree, tc.objectSize)

					b.ResetTimer()
					for k := range b.N {
						_, err := fsTree.GetRange(addrs[k%len(addrs)], tc.from, tc.length)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			})
	}
}
