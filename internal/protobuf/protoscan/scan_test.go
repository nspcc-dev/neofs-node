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
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
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
	{math.MaxInt32, maxInt32Varint},
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

var invalidSHA256Testcases = []invalidDataTestcase{
	{name: "empty", err: "wrong len 0 bytes instead of 32", buf: []byte{}},
	{name: "cut", err: "wrong len 31 bytes instead of 32", buf: testutil.RandByteSlice(31)},
	{name: "overflow", err: "wrong len 33 bytes instead of 32", buf: testutil.RandByteSlice(33)},
	{name: "zero", err: "all bytes are zero", buf: make([]byte, 32)},
}

var invalidNeo3AddressTestcases = []invalidDataTestcase{
	{name: "empty", err: "wrong len 0 bytes instead of 25", buf: []byte{}},
	{name: "cut", err: "wrong len 24 bytes instead of 25", buf: testutil.RandByteSlice(24)},
	{name: "overflow", err: "wrong len 26 bytes instead of 25", buf: testutil.RandByteSlice(26)},
	{name: "wrong prefix", err: "prefix byte is 0x34, expected 0x35", buf: slices.Replace(testutil.RandByteSlice(25), 0, 1, 0x34)},
	{name: "wrong checksum", err: "checksum mismatch", buf: slices.Replace(testutil.RandByteSlice(25), 0, 1, 0x35)},
}

var invalidUUIDV4Testcases = []invalidDataTestcase{
	{name: "empty", err: "wrong len 0 bytes instead of 16", buf: []byte{}},
	{name: "cut", err: "wrong len 15 bytes instead of 16", buf: testutil.RandByteSlice(15)},
	{name: "overflow", err: "wrong len 17 bytes instead of 16", buf: testutil.RandByteSlice(17)},
	{name: "wrong version", err: "wrong version 3 instead of 4", buf: slices.Replace(testutil.RandByteSlice(16), 6, 7, 3<<4)},
}

func TestScanMessage(t *testing.T) {
	t.Run("single field", func(t *testing.T) {
		t.Run("nested", func(t *testing.T) {
			const fieldNum = 42

			scheme := protoscan.MessageScheme{
				Fields: map[protowire.Number]protoscan.MessageField{
					fieldNum: protoscan.NewMessageField("foo", protoscan.FieldTypeNestedMessage),
				},
				NestedMessageAliases: map[protowire.Number]protoscan.SchemeAlias{
					fieldNum: protoscan.SchemeAliasObjectHeader,
				},
			}

			hdr := objecttest.Object().ProtoMessage().GetHeader()
			hdrBin := make([]byte, hdr.MarshaledSize())
			hdr.MarshalStable(hdrBin)

			buf := slices.Concat(protowire.AppendVarint(tagLEN42, uint64(len(hdrBin))), hdrBin)

			var opts protoscan.ScanMessageOptions

			var got []byte
			opts.InterceptNested = func(num protowire.Number, bs iprotobuf.BuffersSlice) error {
				if num != fieldNum {
					return fmt.Errorf("unexpected number %d", num)
				}
				got = bs.ReadOnlyData()
				return protoscan.ErrContinue
			}

			err := protoscan.ScanMessage(newSingleBufferSlice(buf), scheme, opts)
			require.NoError(t, err)
			require.True(t, bytes.Equal(hdrBin, got))
		})
	})
}

func TestScanMessageOrdered(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		for i, bs := range []iprotobuf.BuffersSlice{
			newSingleBufferSlice(nil),
			newSingleBufferSlice([]byte{}),
			iprotobuf.NewBuffersSlice(
				mem.BufferSlice{mem.SliceBuffer{}, mem.SliceBuffer{}, mem.SliceBuffer{}},
			),
		} {
			ordered, err := protoscan.ScanMessageOrdered(bs, protoscan.MessageScheme{}, protoscan.ScanMessageOrderedOptions{})
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

	t.Run("unknown scheme alias", func(t *testing.T) {
		const num = 42
		scheme := protoscan.MessageScheme{
			Fields: map[protowire.Number]protoscan.MessageField{
				num: protoscan.NewMessageField("foo", protoscan.FieldTypeNestedMessage),
			},
			NestedMessageAliases: map[protowire.Number]protoscan.SchemeAlias{
				num: 0,
			},
		}

		require.PanicsWithValue(t, "unexpected alias 0", func() {
			buf := slices.Concat(tagLEN42, []byte{0})
			_, _ = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
		})

		scheme.NestedMessageAliases[num] = 2

		require.PanicsWithValue(t, "unexpected alias 2", func() {
			buf := slices.Concat(tagLEN42, []byte{0})
			_, _ = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
		})
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

	t.Run("single field", func(t *testing.T) {
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

		t.Run("repeated enum", func(t *testing.T) {
			tag := tagWithWireTypeString{
				data:        tagLEN42,
				wireTypeStr: "LEN",
			}
			wrongTag := tagWithWireTypeString{
				data:        tagVARINT42,
				wireTypeStr: "VARINT",
			}

			invalidTestcases := invalidLENTestcases
			for _, tc := range invalidVarint32Testcases {
				if len(tc.buf) == 0 {
					continue
				}
				invalidTestcases = append(invalidTestcases, invalidDataTestcase{
					name: "invalid element/" + tc.name,
					err:  "parse next element: " + tc.err,
					buf: encodePacked(
						[]byte{128, 127}, // 16256
						tc.buf,
					),
				})
			}

			validTestcases := []comparableValueTestcase[any]{
				{
					name: "16256,max,0",
					buf: encodePacked(
						[]byte{128, 127},
						maxInt32Varint,
						[]byte{0},
					),
				},
			}

			testSingleFieldMessageScanningNonIntercepted(t, protoscan.FieldTypeRepeatedEnum, 42, tag, wrongTag, invalidTestcases, validTestcases)
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
			t.Run("unknown binary kind", func(t *testing.T) {
				const num = 42
				scheme := protoscan.MessageScheme{
					Fields: map[protowire.Number]protoscan.MessageField{
						num: protoscan.NewMessageField("foo", protoscan.FieldTypeBytes),
					},
					BinaryFields: map[protowire.Number]protoscan.BinaryFieldKind{
						num: 0,
					},
				}

				require.PanicsWithValue(t, "unexpected binary kind 0", func() {
					buf := slices.Concat(tagLEN42, []byte{0})
					_, _ = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
				})

				scheme.BinaryFields[num] = 4

				require.PanicsWithValue(t, "unexpected binary kind 4", func() {
					buf := slices.Concat(tagLEN42, []byte{0})
					_, _ = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
				})
			})

			t.Run("SHA256", func(t *testing.T) {
				const fieldNum = 42

				scheme := protoscan.MessageScheme{
					Fields: map[protowire.Number]protoscan.MessageField{
						fieldNum: protoscan.NewMessageField("foo", protoscan.FieldTypeBytes),
					},
					BinaryFields: map[protowire.Number]protoscan.BinaryFieldKind{
						fieldNum: protoscan.BinaryFieldKindSHA256,
					},
				}

				for _, tc := range invalidSHA256Testcases {
					t.Run(tc.name, func(t *testing.T) {
						buf := slices.Concat(protowire.AppendVarint(tagLEN42, uint64(len(tc.buf))), tc.buf)

						_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
						require.EqualError(t, err, "parse foo (bytes) field: invalid binary field of SHA256 kind: "+tc.err)
					})
				}

				h := testutil.RandByteSlice(32)

				buf := slices.Concat(protowire.AppendVarint(tagLEN42, 32), h)

				var opts protoscan.ScanMessageOrderedOptions

				ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)

				var got []byte

				opts.InterceptBytes = func(num protowire.Number, bs iprotobuf.BuffersSlice) error {
					if num != fieldNum {
						return fmt.Errorf("unexpected number %d", num)
					}
					got = bs.ReadOnlyData()
					return nil
				}

				ordered, err = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)
				require.True(t, bytes.Equal(h, got))
			})

			t.Run("Neo3 address", func(t *testing.T) {
				const fieldNum = 42

				scheme := protoscan.MessageScheme{
					Fields: map[protowire.Number]protoscan.MessageField{
						fieldNum: protoscan.NewMessageField("foo", protoscan.FieldTypeBytes),
					},
					BinaryFields: map[protowire.Number]protoscan.BinaryFieldKind{
						fieldNum: protoscan.BinaryFieldKindNeo3Address,
					},
				}

				for _, tc := range invalidNeo3AddressTestcases {
					t.Run(tc.name, func(t *testing.T) {
						buf := slices.Concat(protowire.AppendVarint(tagLEN42, uint64(len(tc.buf))), tc.buf)

						_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
						require.EqualError(t, err, "parse foo (bytes) field: invalid binary field of Neo3Address kind: "+tc.err)
					})
				}

				addr := usertest.ID()

				buf := slices.Concat(protowire.AppendVarint(tagLEN42, 25), addr[:])

				var opts protoscan.ScanMessageOrderedOptions

				ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)

				var got []byte

				opts.InterceptBytes = func(num protowire.Number, bs iprotobuf.BuffersSlice) error {
					if num != fieldNum {
						return fmt.Errorf("unexpected number %d", num)
					}
					got = bs.ReadOnlyData()
					return nil
				}

				ordered, err = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)
				require.True(t, bytes.Equal(addr[:], got))
			})

			t.Run("UUIDV4", func(t *testing.T) {
				const fieldNum = 42

				scheme := protoscan.MessageScheme{
					Fields: map[protowire.Number]protoscan.MessageField{
						fieldNum: protoscan.NewMessageField("foo", protoscan.FieldTypeBytes),
					},
					BinaryFields: map[protowire.Number]protoscan.BinaryFieldKind{
						fieldNum: protoscan.BinaryFieldKindUUIDV4,
					},
				}

				for _, tc := range invalidUUIDV4Testcases {
					t.Run(tc.name, func(t *testing.T) {
						buf := slices.Concat(protowire.AppendVarint(tagLEN42, uint64(len(tc.buf))), tc.buf)

						_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
						require.EqualError(t, err, "parse foo (bytes) field: invalid binary field of UUIDV4 kind: "+tc.err)
					})
				}

				uid := testutil.RandByteSlice(16)
				uid[6] = 4 << 4

				buf := slices.Concat(protowire.AppendVarint(tagLEN42, 16), uid)

				var opts protoscan.ScanMessageOrderedOptions

				ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)

				var got []byte

				opts.InterceptBytes = func(num protowire.Number, bs iprotobuf.BuffersSlice) error {
					if num != fieldNum {
						return fmt.Errorf("unexpected number %d", num)
					}
					got = bs.ReadOnlyData()
					return nil
				}

				ordered, err = protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)
				require.True(t, bytes.Equal(uid, got))
			})

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

		t.Run("message", func(t *testing.T) {
			const fieldNum = 42

			scheme := protoscan.MessageScheme{
				Fields: map[protowire.Number]protoscan.MessageField{
					fieldNum: protoscan.NewMessageField("foo", protoscan.FieldTypeNestedMessage),
				},
			}

			t.Run("parse failure", func(t *testing.T) {
				t.Run("len", func(t *testing.T) {
					for _, tc := range invalidLENTestcases {
						t.Run(tc.name, func(t *testing.T) {
							buf := slices.Concat(tagLEN42, tc.buf)

							_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
							require.EqualError(t, err, "parse foo (nested message) field: parse field #42 of LEN type: "+tc.err)
						})
					}
				})

				t.Run("unspecified format", func(t *testing.T) {
					buf := slices.Concat(tagLEN42, []byte{32}, testutil.RandByteSlice(32))

					_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
					require.EqualError(t, err, "format of foo (nested message) field is not specified")
				})

				t.Run("nested message", func(t *testing.T) {
					const otherNum = 123

					scheme.Fields[otherNum] = protoscan.NewMessageField("bar", protoscan.FieldTypeBytes)

					nested := append(protowire.AppendTag(nil, otherNum, protowire.VarintType), 0)

					buf := slices.Concat(protowire.AppendVarint(tagLEN42, uint64(len(nested))), nested)

					t.Run("recursive", func(t *testing.T) {
						scheme.RecursiveField = fieldNum

						_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
						require.EqualError(t, err, "parse foo (nested message) field: parse bar (bytes) field: wrong type of field #123: expected LEN, got VARINT")

						scheme.RecursiveField = 0
					})

					scheme.NestedMessageFields = map[protowire.Number]protoscan.MessageScheme{
						fieldNum: {
							Fields: map[protowire.Number]protoscan.MessageField{
								otherNum: protoscan.NewMessageField("bar", protoscan.FieldTypeBytes),
							},
						},
					}

					_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
					require.EqualError(t, err, "parse foo (nested message) field: parse bar (bytes) field: wrong type of field #123: expected LEN, got VARINT")

					scheme.NestedMessageFields = nil
				})

				t.Run("scheme alias", func(t *testing.T) {
					scheme.NestedMessageAliases = map[protowire.Number]protoscan.SchemeAlias{
						fieldNum: protoscan.SchemeAliasObjectHeader,
					}

					// break version field for example
					nested := append(protowire.AppendTag(nil, 1, protowire.VarintType), 0)

					buf := slices.Concat(protowire.AppendVarint(tagLEN42, uint64(len(nested))), nested)

					_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, protoscan.ScanMessageOrderedOptions{})
					require.EqualError(t, err, "parse foo (nested message) field: parse version (nested message) field: wrong type of field #1: expected LEN, got VARINT")
				})
			})

			hdr := objecttest.Object().ProtoMessage().GetHeader()
			hdrBin := make([]byte, hdr.MarshaledSize())
			hdr.MarshalStable(hdrBin)

			buf := slices.Concat(protowire.AppendVarint(tagLEN42, uint64(len(hdrBin))), hdrBin)

			var opts protoscan.ScanMessageOrderedOptions

			t.Run("interceptor error", func(t *testing.T) {
				testErr := errors.New("some error")

				opts.InterceptNested = func(num protowire.Number, bs iprotobuf.BuffersSlice, checkOrder bool) (bool, error) {
					if num != fieldNum {
						return false, fmt.Errorf("unexpected number %d", num)
					}
					return false, testErr
				}

				_, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.Equal(t, testErr, err)
			})

			t.Run("checking interceptor", func(t *testing.T) {
				var got []byte
				opts.InterceptNested = func(num protowire.Number, bs iprotobuf.BuffersSlice, checkOrder bool) (bool, error) {
					if num != fieldNum {
						return false, fmt.Errorf("unexpected number %d", num)
					}
					got = bs.ReadOnlyData()
					return protoscan.ScanMessageOrdered(bs, protoscan.ObjectHeaderScheme, protoscan.ScanMessageOrderedOptions{})
				}

				ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
				require.NoError(t, err)
				require.True(t, ordered)
				require.True(t, bytes.Equal(hdrBin, got))
			})

			var got []byte
			opts.InterceptNested = func(num protowire.Number, bs iprotobuf.BuffersSlice, checkOrder bool) (bool, error) {
				if num != fieldNum {
					return false, fmt.Errorf("unexpected number %d", num)
				}
				got = bs.ReadOnlyData()
				return checkOrder, protoscan.ErrContinue
			}

			ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
			require.NoError(t, err)
			require.True(t, ordered)
			require.True(t, bytes.Equal(hdrBin, got))
		})
	})

	fld1Uint32 := []byte{8, 152, 158, 213, 220, 12}                      // 3415559960
	fld2Uint64 := []byte{16, 175, 197, 243, 141, 132, 251, 181, 240, 81} // 5899952835671155375
	fld3Enum := []byte{24, 174, 195, 169, 250, 5}                        // 1598710190
	fld4Bool := []byte{32, 1}
	fld5String := []byte{42, 32, 53, 49, 56, 97, 50, 99, 55, 100, 48, 53, 55, 52, 99, 101, 52, 51, 48, 99, 57, 53, 100, 57, 51, 97, 97, 98, 102, 54, 50, 51, 48, 49} // 518a2c7d0574ce430c95d93aabf62301
	fld6Bytes := []byte{50, 20, 210, 152, 14, 215, 167, 135, 229, 168, 121, 197, 98, 11, 169, 55, 67, 41, 158, 189, 22, 83}                                          // value starts after 2 bytes
	fld7RepeatedEnum := []byte{58, 22, 132, 249, 195, 175, 7, 0, 238, 181, 173, 215, 6, 1, 186, 189, 244, 231, 2, 137, 128, 156, 173, 6}                             // 1978727556, 0, 1793809134, 1, 754785978, 1705443337

	orderedSimpleFields := slices.Concat(
		fld1Uint32,
		fld2Uint64,
		fld3Enum,
		fld4Bool,
		fld5String,
		fld6Bytes,
		fld7RepeatedEnum,
	)

	fld8Nested := slices.Concat(
		[]byte{66, 104},
		orderedSimpleFields,
	)

	scheme := protoscan.MessageScheme{
		Fields: map[protowire.Number]protoscan.MessageField{
			1: protoscan.NewMessageField("1-uint32", protoscan.FieldTypeUint32),
			2: protoscan.NewMessageField("2-uint64", protoscan.FieldTypeUint64),
			3: protoscan.NewMessageField("3-enum", protoscan.FieldTypeEnum),
			4: protoscan.NewMessageField("4-bool", protoscan.FieldTypeBool),
			5: protoscan.NewMessageField("5-string", protoscan.FieldTypeString),
			6: protoscan.NewMessageField("6-bytes", protoscan.FieldTypeBytes),
			7: protoscan.NewMessageField("7-repeated-enum", protoscan.FieldTypeRepeatedEnum),
			8: protoscan.NewMessageField("8-nested-message", protoscan.FieldTypeNestedMessage),
		},
		NestedMessageFields: map[protowire.Number]protoscan.MessageScheme{
			8: {
				Fields: map[protowire.Number]protoscan.MessageField{ // recursion is not a goal, it's just simpler
					1: protoscan.NewMessageField("1-uint32", protoscan.FieldTypeUint32),
					2: protoscan.NewMessageField("2-uint64", protoscan.FieldTypeUint64),
					3: protoscan.NewMessageField("3-enum", protoscan.FieldTypeEnum),
					4: protoscan.NewMessageField("4-bool", protoscan.FieldTypeBool),
					5: protoscan.NewMessageField("5-string", protoscan.FieldTypeString),
					6: protoscan.NewMessageField("6-bytes", protoscan.FieldTypeBytes),
					7: protoscan.NewMessageField("7-repeated-enum", protoscan.FieldTypeRepeatedEnum),
				},
			},
		},
	}

	var gotUint32 uint32
	var gotUint64 uint64
	var gotEnum int32
	var gotBool bool
	var gotString string
	var gotBytes []byte
	var gotNested []byte

	var opts protoscan.ScanMessageOrderedOptions
	opts.InterceptUint32 = func(num protowire.Number, u uint32) error {
		if num != 1 {
			return fmt.Errorf("unexpected uint32 field with number %d", num)
		}
		gotUint32 = u
		return nil
	}
	opts.InterceptUint64 = func(num protowire.Number, u uint64) error {
		if num != 2 {
			return fmt.Errorf("unexpected uint64 field with number %d", num)
		}
		gotUint64 = u
		return nil
	}
	opts.InterceptEnum = func(num protowire.Number, e int32) error {
		if num != 3 {
			return fmt.Errorf("unexpected enum field with number %d", num)
		}
		gotEnum = e
		return nil
	}
	opts.InterceptBool = func(num protowire.Number, b bool) error {
		if num != 4 {
			return fmt.Errorf("unexpected bool field with number %d", num)
		}
		gotBool = b
		return nil
	}
	opts.InterceptString = func(num protowire.Number, s []byte) error {
		if num != 5 {
			return fmt.Errorf("unexpected string field with number %d", num)
		}
		gotString = string(s)
		return nil
	}
	opts.InterceptBytes = func(num protowire.Number, bs iprotobuf.BuffersSlice) error {
		if num != 6 {
			return fmt.Errorf("unexpected bytes field with number %d", num)
		}
		gotBytes = bs.ReadOnlyData()
		return nil
	}
	opts.InterceptNested = func(num protowire.Number, bs iprotobuf.BuffersSlice, checkOrder bool) (bool, error) {
		if num != 8 {
			return false, fmt.Errorf("unexpected nested message field with number %d", num)
		}
		gotNested = bs.ReadOnlyData()
		return checkOrder, protoscan.ErrContinue
	}

	checkOK := func(t *testing.T, err error) {
		require.NoError(t, err)
		require.EqualValues(t, 3415559960, gotUint32)
		require.EqualValues(t, 5899952835671155375, gotUint64)
		require.EqualValues(t, 1598710190, gotEnum)
		require.True(t, gotBool)
		require.Equal(t, "518a2c7d0574ce430c95d93aabf62301", gotString)
		require.True(t, bytes.Equal(gotBytes, fld6Bytes[2:]))

		gotUint32 = 0
		gotUint64 = 0
		gotEnum = 0
		gotBool = false
		gotString = ""
		gotBytes = nil
	}

	t.Run("unordered", func(t *testing.T) {
		t.Run("nested", func(t *testing.T) {
			fld8Nested := slices.Concat(
				[]byte{66, 104},
				fld1Uint32,
				fld2Uint64,
				fld3Enum,
				fld4Bool,
				fld6Bytes,
				fld5String,
				fld7RepeatedEnum,
			)

			buf := slices.Concat(
				fld1Uint32,
				fld2Uint64,
				fld3Enum,
				fld5String,
				fld4Bool,
				fld6Bytes,
				fld7RepeatedEnum,
				fld8Nested,
			)

			ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
			checkOK(t, err)
			require.False(t, ordered)
			require.True(t, bytes.Equal(gotNested, fld8Nested[2:]))
			gotNested = nil
		})

		buf := slices.Concat(
			fld1Uint32,
			fld2Uint64,
			fld3Enum,
			fld5String,
			fld4Bool,
			fld6Bytes,
			fld7RepeatedEnum,
			fld8Nested,
		)

		ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
		checkOK(t, err)
		require.False(t, ordered)
		require.True(t, bytes.Equal(gotNested, fld8Nested[2:]))
		gotNested = nil
	})

	buf := slices.Concat(
		orderedSimpleFields,
		fld8Nested,
	)

	ordered, err := protoscan.ScanMessageOrdered(newSingleBufferSlice(buf), scheme, opts)
	checkOK(t, err)
	require.True(t, ordered)
	require.True(t, bytes.Equal(gotNested, fld8Nested[2:]))
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

func testSingleFieldMessageScanningNonIntercepted[T any](t *testing.T, fieldType protoscan.FieldType, fieldNum protowire.Number, tag, wrongTag tagWithWireTypeString,
	invalidTestcases []invalidDataTestcase, validTestcases []comparableValueTestcase[T]) {
	testSingleFieldMessageScanningWithCustomInterceptAssert[T, any](t, fieldType, fieldNum, tag, wrongTag, invalidTestcases, validTestcases, nil, nil)
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

			if setInterceptFn == nil {
				return
			}

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
			require.Equal(t, testErr, err)
		})
	}
}
