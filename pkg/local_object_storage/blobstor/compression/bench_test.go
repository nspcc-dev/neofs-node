package compression

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkCompression(b *testing.B) {
	c := Config{Enabled: true}
	require.NoError(b, c.Init())

	for _, size := range []int{128, 1024, 32 * 1024, 32 * 1024 * 1024} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.Run("zeroed slice", func(b *testing.B) {
				data := make([]byte, size)
				benchWith(b, c, data)
			})
			b.Run("not so random slice (block = 123)", func(b *testing.B) {
				data := notSoRandomSlice(size, 123)
				benchWith(b, c, data)
			})
			b.Run("random slice", func(b *testing.B) {
				data := make([]byte, size)
				_, _ = rand.Read(data)
				benchWith(b, c, data)
			})
		})
	}
}

func benchWith(b *testing.B, c Config, data []byte) {
	b.ReportAllocs()
	for b.Loop() {
		_ = c.Compress(data)
	}
}

func notSoRandomSlice(size, blockSize int) []byte {
	data := make([]byte, size)
	_, _ = rand.Read(data[:blockSize])
	for i := blockSize; i < size; i += blockSize {
		copy(data[i:], data[:blockSize])
	}
	return data
}
