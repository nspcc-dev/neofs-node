package protoscan_test

import (
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
)

func TestObject(t *testing.T) {
	obj := newTestChildObject(t)

	buf := obj.Marshal()

	t.Run("ignore order", func(t *testing.T) {
		err := protoscan.ScanMessage(newSingleBufferSlice(buf), protoscan.ObjectScheme, protoscan.ScanMessageOptions{})
		require.NoError(t, err)

		err = protoscan.ScanMessage(byteByByteSlice(buf), protoscan.ObjectScheme, protoscan.ScanMessageOptions{})
		require.NoError(t, err)
	})

	ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), protoscan.ObjectScheme, protoscan.ScanMessageOrderedOptions{})
	require.NoError(t, err)
	require.True(t, ordered)

	ordered, err = protoscan.ScanMessageOrdered(byteByByteSlice(buf), protoscan.ObjectScheme, protoscan.ScanMessageOrderedOptions{})
	require.NoError(t, err)
	require.True(t, ordered)
}

func BenchmarkObject(b *testing.B) {
	obj := newTestChildObject(b)

	buf := obj.Marshal()

	bufSlice := mem.BufferSlice{mem.SliceBuffer(buf)}

	b.Run("ignore order", func(b *testing.B) {
		for b.Loop() {
			err := protoscan.ScanMessage(iprotobuf.NewBuffersSlice(bufSlice), protoscan.ObjectScheme, protoscan.ScanMessageOptions{})
			require.NoError(b, err)
		}
	})

	b.Run("check order", func(b *testing.B) {
		for b.Loop() {
			ordered, err := protoscan.ScanMessageOrdered(iprotobuf.NewBuffersSlice(bufSlice), protoscan.ObjectScheme, protoscan.ScanMessageOrderedOptions{})
			require.NoError(b, err)
			require.True(b, ordered)
		}
	})
}
