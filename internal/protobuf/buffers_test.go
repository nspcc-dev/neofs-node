package protobuf_test

import (
	"io"
	"math"
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestBuffersSlice_Zero(t *testing.T) {
	var bs iprotobuf.BuffersSlice

	require.True(t, bs.IsEmpty())

	require.Empty(t, bs.ReadOnlyData())

	sub, ok := bs.MoveNext(0)
	require.True(t, ok)
	require.True(t, sub.IsEmpty())

	_, ok = bs.MoveNext(1)
	require.False(t, ok)

	_, _, err := bs.ParseTag()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = bs.ParseLENField(0, protowire.BytesType)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = bs.ParseBoolField(0, protowire.VarintType)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = bs.ParseEnumField(0, protowire.VarintType)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = bs.ParseStringField(0, protowire.BytesType)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = bs.ParseUint32Field(0, protowire.VarintType)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	_, err = bs.ParseUint64Field(0, protowire.VarintType)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestBuffersSlice_IsEmpty(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(nil)
		require.True(t, bs.IsEmpty())
	})

	t.Run("empty", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{})
		require.True(t, bs.IsEmpty())
	})

	t.Run("multiple empty", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{
			mem.SliceBuffer{},
			mem.SliceBuffer(nil),
		})
		require.True(t, bs.IsEmpty())
	})

	bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{
		mem.SliceBuffer{},
		mem.SliceBuffer{1},
		mem.SliceBuffer{},
	})
	require.False(t, bs.IsEmpty())
}

func TestBuffersSlice_ReadOnlyData(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(nil)
		require.Empty(t, bs.ReadOnlyData())
	})

	t.Run("empty", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{})
		require.Empty(t, bs.ReadOnlyData())
	})

	t.Run("multiple empty", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{
			mem.SliceBuffer{},
			mem.SliceBuffer(nil),
		})
		require.Empty(t, bs.ReadOnlyData())
	})

	t.Run("single buffer", func(t *testing.T) {
		buf := mem.SliceBuffer{0, 1, 2}
		bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{buf})
		got := bs.ReadOnlyData()
		require.EqualValues(t, buf, got)
		// mutate buffer to make sure it is not copied
		got[0] = 255
		require.EqualValues(t, 255, buf[0])
	})

	buf1 := mem.SliceBuffer{0, 1, 2}
	buf2 := mem.SliceBuffer{3}

	bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{
		mem.SliceBuffer{},
		buf1,
		mem.SliceBuffer{},
		buf2,
		mem.SliceBuffer{},
	})

	got := bs.ReadOnlyData()
	require.Equal(t, []byte{0, 1, 2, 3}, got)
	got[0]++
	require.EqualValues(t, 0, buf1[0])
}

func TestBuffersSlice_MoveNext(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(nil)

		sub, ok := bs.MoveNext(0)
		require.True(t, ok)
		require.True(t, sub.IsEmpty())

		_, ok = bs.MoveNext(1)
		require.False(t, ok)
	})

	t.Run("empty", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{})

		sub, ok := bs.MoveNext(0)
		require.True(t, ok)
		require.True(t, sub.IsEmpty())

		_, ok = bs.MoveNext(1)
		require.False(t, ok)
	})

	t.Run("multiple empty", func(t *testing.T) {
		bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{
			mem.SliceBuffer{},
			mem.SliceBuffer(nil),
		})

		sub, ok := bs.MoveNext(0)
		require.True(t, ok)
		require.True(t, sub.IsEmpty())

		_, ok = bs.MoveNext(1)
		require.False(t, ok)
	})

	t.Run("single buffer", func(t *testing.T) {
		buf := mem.SliceBuffer{0, 1, 2, 3}
		bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{buf})

		sub, ok := bs.MoveNext(2)
		require.True(t, ok)
		require.Equal(t, []byte{0, 1}, sub.ReadOnlyData())
		require.Equal(t, []byte{2, 3}, bs.ReadOnlyData())

		// mutate buffer to make sure it is not copied
		sub.ReadOnlyData()[0] = 254
		require.EqualValues(t, 254, buf[0])
		bs.ReadOnlyData()[0] = 255
		require.EqualValues(t, 255, buf[2])

		sub, ok = bs.MoveNext(2)
		require.True(t, ok)
		require.Equal(t, []byte{255, 3}, sub.ReadOnlyData())

		require.True(t, bs.IsEmpty())

		_, ok = bs.MoveNext(1)
		require.False(t, ok)
	})

	buf1 := mem.SliceBuffer{0, 1, 2}
	buf2 := mem.SliceBuffer{3, 4, 5}

	bs := iprotobuf.NewBuffersSlice(mem.BufferSlice{
		mem.SliceBuffer{},
		buf1,
		mem.SliceBuffer{},
		buf2,
		mem.SliceBuffer{},
	})

	sub, ok := bs.MoveNext(2)
	require.True(t, ok)
	require.Equal(t, []byte{0, 1}, sub.ReadOnlyData())
	require.Equal(t, []byte{2, 3, 4, 5}, bs.ReadOnlyData())
}

func TestBuffersSlice_ParseUint64Field(t *testing.T) {
	check := func(t *testing.T, s [][]byte) {
		buffers := make(mem.BufferSlice, len(s))
		for i := range s {
			buffers[i] = mem.SliceBuffer(s[i])
		}

		bs := iprotobuf.NewBuffersSlice(buffers)

		got, err := bs.ParseUint64Field(1, protowire.VarintType)
		require.NoError(t, err)
		require.Equal(t, uint64(math.MaxUint64), got)
	}

	t.Run("byte-by-byte", func(t *testing.T) {
		var s [][]byte
		for _, b := range maxUint64Varint {
			s = append(s,
				[]byte{},
				[]byte{b},
				[]byte{},
			)
		}

		s = append(s, []byte("anything"))

		check(t, s)
	})

	t.Run("two buffers", func(t *testing.T) {
		ln := len(maxUint64Varint)
		check(t, [][]byte{
			maxUint64Varint[:ln/2],
			append(maxUint64Varint[ln/2:], "anything"...),
		})
	})

	check(t, [][]byte{
		append(maxUint64Varint, "anything"...),
	})

	check(t, [][]byte{
		{},
		maxUint64Varint,
	})
}
