package protoscan_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/protobuf/protoscan"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/encoding/protowire"
)

type comparableValueTestcase[T any] struct {
	name  string
	value T
	buf   []byte
}

var uvarintTestcases = []struct {
	value uint64
	buf   []byte
}{
	{0, []byte{0}},
	{127, []byte{127}},
	{128, []byte{128, 1}},
	{16256, []byte{128, 127}},
	{16384, []byte{128, 128, 1}},
	{math.MaxInt32, []byte{255, 255, 255, 255, 7}},
	{math.MaxUint32, []byte{255, 255, 255, 255, 15}},
	{math.MaxUint64, []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
}

type invalidDataTestcase struct {
	name string
	err  string
	buf  []byte
}

var commonInvalidVarintTestcases = []invalidDataTestcase{
	{name: "empty", err: "parse varint: unexpected EOF", buf: []byte{}},
	{name: "truncated", err: "parse varint: unexpected EOF", buf: []byte{128}},
}

var invalidUvarint64Testcases = append(commonInvalidVarintTestcases,
	invalidDataTestcase{name: "overflow", err: "parse varint: variable length integer overflow", buf: uint64OverflowVarint},
)

var invalidUvarint32Testcases = append(commonInvalidVarintTestcases,
	invalidDataTestcase{name: "overflow", err: "value 4294967296 overflows uint32", buf: uint32OverflowVarint},
)

var invalidVarint32Testcases = append(commonInvalidVarintTestcases,
	invalidDataTestcase{name: "negative", err: "negative value -1", buf: negativeVarint},
	invalidDataTestcase{name: "overflow", err: "value 2147483648 overflows int32", buf: int32OverflowVarint},
)

// set in init.
var invalidLENTestcases = []invalidDataTestcase{
	{name: "overflow", err: "value 9223372036854775808 overflows int", buf: int64OverflowVarint},
}

func init() {
	for _, tc := range invalidUvarint64Testcases {
		invalidLENTestcases = append(invalidLENTestcases, invalidDataTestcase{
			name: "invalid len/" + tc.name,
			err:  tc.err,
			buf:  tc.buf,
		})
	}
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

	t.Run("unsupported field", func(t *testing.T) {
		scheme := protoscan.MessageScheme{
			Fields: map[protowire.Number]protoscan.MessageField{
				1: protoscan.NewMessageField("test field", protoscan.FieldTypeString),
			},
		}
		buf := []byte{208, 2, 0} // VARINT#42 = 0
		_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
		require.EqualError(t, err, "unsupported field #42 of type VARINT")
	})

	t.Run("invalid tag", func(t *testing.T) {
		t.Run("invalid varint", func(t *testing.T) {
			for _, tc := range invalidUvarint64Testcases {
				if len(tc.buf) == 0 {
					continue
				}
				t.Run(tc.name, func(t *testing.T) {
					_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(tc.buf), protoscan.MessageScheme{}, protoscan.ScanMessageOrderedOptions{})
					require.EqualError(t, err, "parse next tag: "+tc.err)
				})
			}
		})

		t.Run("invalid number", func(t *testing.T) {
			for _, tc := range []struct {
				name string
				num  int
				buf  []byte
			}{
				{name: "0", num: 0, buf: []byte{2}},
				{name: "negative", num: -1, buf: negativeVarint},
				{name: "too big", num: 536870912, buf: []byte{130, 128, 128, 128, 16}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(tc.buf), protoscan.MessageScheme{}, protoscan.ScanMessageOrderedOptions{})
					require.EqualError(t, err, "parse next tag: invalid number "+strconv.Itoa(tc.num))
				})
			}
		})
	})

	t.Run("single field", testSingleFIeldScanMessageScanning)
}

func testSingleFIeldScanMessageScanning(t *testing.T) {
	t.Run("uint32", func(t *testing.T) {
		var validTestcases []comparableValueTestcase[uint32]
		for _, tc := range uvarintTestcases {
			if tc.value > math.MaxUint32 {
				continue
			}
			validTestcases = append(validTestcases, comparableValueTestcase[uint32]{
				name:  strconv.FormatUint(tc.value, 10),
				value: uint32(tc.value),
				buf:   tc.buf,
			})
		}

		testSingleVarintFieldMessageScanning(t, protoscan.FieldTypeUint32, invalidUvarint32Testcases, validTestcases, func(opts *protoscan.ScanMessageOrderedOptions, fn func(protowire.Number, uint32) error) {
			opts.InterceptUint32 = fn
		})
	})

	t.Run("uint64", func(t *testing.T) {
		var validTestcases []comparableValueTestcase[uint64]
		for _, tc := range uvarintTestcases {
			if tc.value > math.MaxUint32 {
				continue
			}
			validTestcases = append(validTestcases, comparableValueTestcase[uint64]{
				name:  strconv.FormatUint(tc.value, 10),
				value: tc.value,
				buf:   tc.buf,
			})
		}

		testSingleVarintFieldMessageScanning(t, protoscan.FieldTypeUint64, invalidUvarint64Testcases, validTestcases, func(opts *protoscan.ScanMessageOrderedOptions, fn func(protowire.Number, uint64) error) {
			opts.InterceptUint64 = fn
		})
	})

	t.Run("enum", func(t *testing.T) {
		var validTestcases []comparableValueTestcase[int32]
		for _, tc := range uvarintTestcases {
			if tc.value > math.MaxInt32 {
				continue
			}
			validTestcases = append(validTestcases, comparableValueTestcase[int32]{
				name:  strconv.FormatUint(tc.value, 10),
				value: int32(tc.value),
				buf:   tc.buf,
			})
		}

		testSingleVarintFieldMessageScanning(t, protoscan.FieldTypeEnum, invalidVarint32Testcases, validTestcases, func(opts *protoscan.ScanMessageOrderedOptions, fn func(protowire.Number, int32) error) {
			opts.InterceptEnum = fn
		})
	})

	t.Run("bool", func(t *testing.T) {
		validTestcases := []comparableValueTestcase[bool]{
			{name: "false", value: false, buf: []byte{0}},
			{name: "true", value: true, buf: []byte{1}},
		}

		invalidTestcases := []invalidDataTestcase{
			{name: "empty", err: "unexpected EOF", buf: []byte{}},
			{name: "invalid byte", err: "invalid byte 2", buf: []byte{2}},
		}

		testSingleVarintFieldMessageScanning(t, protoscan.FieldTypeBool, invalidTestcases, validTestcases, func(opts *protoscan.ScanMessageOrderedOptions, fn func(protowire.Number, bool) error) {
			opts.InterceptBool = fn
		})
	})

	t.Run("string", func(t *testing.T) {
		tag := tagWithWireTypeString{
			data:        tagLEN42,
			wireTypeStr: "LEN",
		}
		wrongTag := tagWithWireTypeString{
			data:        tagVARINT42,
			wireTypeStr: "VARINT",
		}

		invalidTestcases := append(invalidLENTestcases,
			invalidDataTestcase{name: "invalid UTF-8", err: "invalid UTF-8", buf: append([]byte{5}, "\xFB\xBF\xBF\xBF\xBF"...)}, // example from utf8 package
		)

		var validTestcases []comparableValueTestcase[string]
		for _, tc := range uvarintTestcases {
			if tc.value > 100<<10 || tc.value%2 != 0 {
				continue
			}
			str := hex.EncodeToString(testutil.RandByteSlice(tc.value / 2))
			validTestcases = append(validTestcases, comparableValueTestcase[string]{
				name:  "len=" + strconv.Itoa(len(str)),
				value: str,
				buf:   append(tc.buf, str...),
			})
		}

		testSingleFieldMessageScanning(t, protoscan.FieldTypeString, 42, tag, wrongTag, invalidTestcases, validTestcases, func(opts *protoscan.ScanMessageOrderedOptions, fn func(protowire.Number, []byte) error) {
			opts.InterceptString = fn
		})
	})

	t.Run("bytes", func(t *testing.T) {
		tag := tagWithWireTypeString{
			data:        tagLEN42,
			wireTypeStr: "LEN",
		}
		wrongTag := tagWithWireTypeString{
			data:        tagVARINT42,
			wireTypeStr: "VARINT",
		}

		var validTestcases []comparableValueTestcase[[]byte]
		for _, tc := range uvarintTestcases {
			if tc.value > 100<<10 {
				continue
			}
			b := testutil.RandByteSlice(tc.value)
			validTestcases = append(validTestcases, comparableValueTestcase[[]byte]{
				name:  "len=" + strconv.Itoa(len(b)),
				value: b,
				buf:   append(tc.buf, b...),
			})
		}

		testSingleFieldMessageScanningWithCustomInterceptAssert(t, protoscan.FieldTypeBytes, 42, tag, wrongTag, invalidLENTestcases, validTestcases, func(opts *protoscan.ScanMessageOrderedOptions, fn func(protowire.Number, iprotobuf.BuffersSlice) error) {
			opts.InterceptBytes = fn
		}, func(t *testing.T, exp []byte, got iprotobuf.BuffersSlice) {
			require.True(t, bytes.Equal(exp, got.ReadOnlyData()))
		})
	})
}

func testSingleVarintFieldMessageScanning[T comparable](t *testing.T, fieldType protoscan.FieldType, invalidTestcases []invalidDataTestcase,
	validTestcases []comparableValueTestcase[T], setInterceptFn func(*protoscan.ScanMessageOrderedOptions, func(protowire.Number, T) error)) {
	tag := tagWithWireTypeString{
		data:        tagVARINT42,
		wireTypeStr: "VARINT",
	}
	wrongTag := tagWithWireTypeString{
		data:        tagLEN42,
		wireTypeStr: "LEN",
	}
	testSingleFieldMessageScanning(t, fieldType, 42, tag, wrongTag, invalidTestcases, validTestcases, setInterceptFn)
}

type tagWithWireTypeString struct {
	data        []byte
	wireTypeStr string
}

func testSingleFieldMessageScanning[T, T2 any](t *testing.T, fieldType protoscan.FieldType, fieldNum protowire.Number, tag, wrongTag tagWithWireTypeString,
	invalidTestcases []invalidDataTestcase, validTestcases []comparableValueTestcase[T], setInterceptFn func(*protoscan.ScanMessageOrderedOptions, func(protowire.Number, T2) error)) {
	testSingleFieldMessageScanningWithCustomInterceptAssert(t, fieldType, fieldNum, tag, wrongTag, invalidTestcases, validTestcases, setInterceptFn, nil)
}

func testSingleFieldMessageScanningWithCustomInterceptAssert[T, T2 any](t *testing.T, fieldType protoscan.FieldType, fieldNum protowire.Number, tag, wrongTag tagWithWireTypeString,
	invalidTestcases []invalidDataTestcase, validTestcases []comparableValueTestcase[T], setInterceptFn func(*protoscan.ScanMessageOrderedOptions, func(protowire.Number, T2) error),
	cmpIntercepted func(t *testing.T, exp T, got T2)) {
	scheme := protoscan.MessageScheme{
		Fields: map[protowire.Number]protoscan.MessageField{
			fieldNum: protoscan.NewMessageField("test field", fieldType),
		},
	}

	t.Run("wrong type", func(t *testing.T) {
		_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(wrongTag.data), scheme, protoscan.ScanMessageOrderedOptions{})
		require.EqualError(t, err, fmt.Sprintf("parse test field (%s) field: wrong type of field #%d: expected %s, got %s",
			fieldType, fieldNum, tag.wireTypeStr, wrongTag.wireTypeStr))
	})

	t.Run("invalid value", func(t *testing.T) {
		for _, tc := range invalidTestcases {
			t.Run(tc.name, func(t *testing.T) {
				buf := slices.Concat(tag.data, tc.buf)
				_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
				require.EqualError(t, err, fmt.Sprintf("parse test field (%s) field: parse field #%d of %s type: %s",
					fieldType, fieldNum, tag.wireTypeStr, tc.err))
			})
		}
	})

	for _, tc := range validTestcases {
		t.Run(tc.name, func(t *testing.T) {
			buf := slices.Concat(tag.data, tc.buf)

			var opts protoscan.ScanMessageOrderedOptions
			ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
			require.NoError(t, err)
			require.True(t, ordered)

			var got T2
			var intercepted bool
			setInterceptFn(&opts, func(num protowire.Number, val T2) error {
				if num != fieldNum {
					return fmt.Errorf("unexpected number %d", num)
				}
				got = val
				intercepted = true
				return nil
			})

			ordered, err = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
			require.NoError(t, err)
			require.True(t, ordered)
			require.True(t, intercepted)
			if cmpIntercepted != nil {
				cmpIntercepted(t, tc.value, got)
			} else {
				require.EqualValues(t, tc.value, got)
			}

			testErr := errors.New("any interceptor error")
			setInterceptFn(&opts, func(protowire.Number, T2) error {
				return testErr
			})
			_, err = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
			require.Equal(t, err, testErr)
		})
	}
}

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
