package protoscan_test

import (
	"math"
	"slices"
	"strconv"
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

var (
	int32OverflowVarint  = []byte{128, 128, 128, 128, 8}  // 2147483648
	uint32OverflowVarint = []byte{128, 128, 128, 128, 16} // 4294967296

	uint64OverflowVarint = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 2}

	tagVarint42 = []byte{208, 2}
)

var varintTestcases = []struct {
	num uint64
	buf []byte
}{
	{0, []byte{0}},
	{127, []byte{127}},
	{128, []byte{128, 1}},
	{16256, []byte{128, 127}},
	{16384, []byte{128, 128, 1}},
	{math.MaxUint64, []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
}

var invalidVarintTestcases = []struct {
	name string
	err  string
	buf  []byte
}{
	{name: "empty buffer", err: "unexpected EOF", buf: []byte{}},
	{name: "truncated", err: "unexpected EOF", buf: []byte{128}},
	{name: "overflow", err: "variable length integer overflow", buf: uint64OverflowVarint},
}

func TestScanMessageOrdered(t *testing.T) {
	var anyScheme protoscan.MessageScheme

	t.Run("empty", func(t *testing.T) {
		for i, bs := range []iprotobuf.BuffersSlice{
			newSingleBufferSlice(nil),
			newSingleBufferSlice([]byte{}),
			iprotobuf.NewBuffersSlice(
				mem.BufferSlice{mem.SliceBuffer{}, mem.SliceBuffer{}, mem.SliceBuffer{}},
			),
		} {
			ordered, err := protoscan.ScanMessageOrdered(bs, anyScheme, protoscan.ScanMessageOrderedOptions{})
			require.NoError(t, err, i)
			require.True(t, ordered, i)
		}
	})

	t.Run("invalid tag", func(t *testing.T) {
		t.Run("invalid varint", func(t *testing.T) {
			for _, tc := range invalidVarintTestcases {
				t.Run(tc.name, func(t *testing.T) {
					_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(tc.buf), protoscan.MessageScheme{}, protoscan.ScanMessageOrderedOptions{})
					require.ErrorContains(t, err, "parse varint")
					require.ErrorContains(t, err, tc.err)
				})
			}
		})

		t.Run("invalid number", func(t *testing.T) {
			t.Run("0", func(t *testing.T) {
				_, _, _, err := iprotobuf.ParseTag([]byte{2}) // 0,LEN
				require.EqualError(t, err, "invalid number 0")
			})
			t.Run("negative", func(t *testing.T) {
				_, _, _, err := iprotobuf.ParseTag([]byte{250, 255, 255, 255, 255, 255, 255, 255, 255, 1}) // -1,LEN
				require.EqualError(t, err, "invalid number -1")
			})
			t.Run("too big", func(t *testing.T) {
				_, _, _, err := iprotobuf.ParseTag([]byte{130, 128, 128, 128, 16}) // 536870912,LEN
				require.EqualError(t, err, "invalid number 536870912")
			})
		})
	})

	t.Run("uint64", func(t *testing.T) {
		scheme := protoscan.MessageScheme{
			Fields: map[protowire.Number]protoscan.MessageField{
				42: protoscan.NewMessageField("test name", protoscan.FieldTypeUint64),
			},
		}

		t.Run("invalid", func(t *testing.T) {
			for _, tc := range invalidVarintTestcases {
				t.Run(tc.name, func(t *testing.T) {
					buf := slices.Concat(tagVarint42, tc.buf)

					var opts protoscan.ScanMessageOrderedOptions
					_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
					require.EqualError(t, err, "parse test name (uint64) field: parse field #42 of VARINT type: parse varint: "+tc.err)
				})
			}
		})

		for _, tc := range varintTestcases {
			t.Run(strconv.FormatUint(tc.num, 10), func(t *testing.T) {
				buf := slices.Concat(tagVarint42, tc.buf)

				var opts protoscan.ScanMessageOrderedOptions
				ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)

				var got uint64
				opts.InterceptUint64 = func(num protowire.Number, u uint64) error {
					if num == 42 {
						got = u
					}
					return nil
				}

				ordered, err = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)
				require.EqualValues(t, tc.num, got)
			})
		}
	})
}
