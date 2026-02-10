package protobuf_test

import (
	"encoding/binary"
	"math"
	"slices"
	"strconv"
	"testing"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

var (
	int32OverflowVarint  = []byte{128, 128, 128, 128, 8}  // 2147483648
	uint32OverflowVarint = []byte{128, 128, 128, 128, 16} // 4294967296

	uint64OverflowVarint = []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 2}
)

var varintTestcases = []struct {
	val uint64
	ln  int
	buf []byte
}{
	{val: 0, ln: 1, buf: []byte{0}},
	{val: 127, ln: 1, buf: []byte{127}},
	{val: 128, ln: 2, buf: []byte{128, 1}},
	{val: 16256, ln: 2, buf: []byte{128, 127}},
	{val: 16384, ln: 3, buf: []byte{128, 128, 1}},
	{val: math.MaxUint64, ln: 10, buf: []byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 1}},
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

func TestParseVarint(t *testing.T) {
	for _, tc := range invalidVarintTestcases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := iprotobuf.ParseVarint(tc.buf)
			require.ErrorContains(t, err, tc.err)
		})
	}

	for i, tc := range varintTestcases {
		u, n, err := iprotobuf.ParseVarint(tc.buf)
		require.NoError(t, err, i)
		require.EqualValues(t, tc.val, u, i)
		require.EqualValues(t, tc.ln, n, i)
	}
}

func TestParseTag(t *testing.T) {
	t.Run("invalid varint", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, _, err := iprotobuf.ParseTag(tc.buf)
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

	check := func(t *testing.T, buf []byte, expNum protowire.Number, expTyp protowire.Type, expN int) {
		num, typ, n, err := iprotobuf.ParseTag(buf)
		require.NoError(t, err)
		require.EqualValues(t, expNum, num)
		require.EqualValues(t, expTyp, typ)
		require.EqualValues(t, expN, n)
	}

	check(t, []byte{130, 64}, 1024, protowire.BytesType, 2)
	check(t, []byte{24, 1, 2, 3}, 3, protowire.VarintType, 1)
}

func TestParseLEN(t *testing.T) {
	t.Run("invalid len", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := iprotobuf.ParseLEN(tc.buf)
				require.ErrorContains(t, err, "parse varint")
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("buffer overflow", func(t *testing.T) {
		t.Run("int overflow", func(t *testing.T) {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, uint64(math.MaxInt+1))

			_, _, err := iprotobuf.ParseLEN(buf[:n])
			require.EqualError(t, err, "value "+strconv.FormatUint(math.MaxInt+1, 10)+" overflows int")
		})

		for i, tc := range varintTestcases {
			if tc.val == 0 || tc.val > 1<<20 {
				continue
			}

			buf := slices.Concat(tc.buf, make([]byte, tc.val-1))
			_, _, err := iprotobuf.ParseLEN(buf)
			require.EqualError(t, err, "unexpected EOF: need "+strconv.FormatUint(tc.val, 10)+" bytes, left "+strconv.FormatUint(tc.val-1, 10)+" in buffer", i)
		}
	})

	for i, tc := range varintTestcases {
		if tc.val > 1<<20 {
			continue
		}

		buf := make([]byte, tc.val)
		u, n, err := iprotobuf.ParseLEN(slices.Concat(tc.buf, buf))
		require.NoError(t, err, i)
		require.EqualValues(t, tc.val, u, i)
		require.EqualValues(t, tc.ln, n, i)
	}
}

func TestParseLENField(t *testing.T) {
	t.Run("wrong type", func(t *testing.T) {
		_, _, err := iprotobuf.ParseLENField([]byte{}, 42, protowire.VarintType)
		require.EqualError(t, err, "wrong type of field #42: expected LEN, got VARINT")
	})

	t.Run("invalid len", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := iprotobuf.ParseLENField(tc.buf, 42, protowire.BytesType)
				require.ErrorContains(t, err, "parse field #42 of LEN type: parse varint")
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("buffer overflow", func(t *testing.T) {
		t.Run("int overflow", func(t *testing.T) {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, uint64(math.MaxInt+1))

			_, _, err := iprotobuf.ParseLENField(buf[:n], 42, protowire.BytesType)
			require.EqualError(t, err, "parse field #42 of LEN type: value "+strconv.FormatUint(math.MaxInt+1, 10)+" overflows int")
		})

		for i, tc := range varintTestcases {
			if tc.val == 0 || tc.val > 1<<20 {
				continue
			}

			buf := slices.Concat(tc.buf, make([]byte, tc.val-1))
			_, _, err := iprotobuf.ParseLENField(buf, 42, protowire.BytesType)
			require.EqualError(t, err, "parse field #42 of LEN type: unexpected EOF: need "+strconv.FormatUint(tc.val, 10)+" bytes, left "+strconv.FormatUint(tc.val-1, 10)+" in buffer", i)
		}
	})

	for i, tc := range varintTestcases {
		if tc.val > 1<<20 {
			continue
		}

		buf := make([]byte, tc.val)
		u, n, err := iprotobuf.ParseLENField(slices.Concat(tc.buf, buf), 42, protowire.BytesType)
		require.NoError(t, err, i)
		require.EqualValues(t, tc.val, u, i)
		require.EqualValues(t, tc.ln, n, i)
	}
}

func TestParseLENFieldBounds(t *testing.T) {
	prefix := []byte{1, 2, 3, 4}
	const off = 2
	tagLn := len(prefix) - off

	t.Run("cut prefix", func(t *testing.T) {
		require.Panics(t, func() {
			_, _ = iprotobuf.ParseLENFieldBounds(prefix, off+1, tagLn, 42, protowire.BytesType)
		})
		require.Panics(t, func() {
			_, _ = iprotobuf.ParseLENFieldBounds(prefix, off, tagLn+1, 42, protowire.BytesType)
		})
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := iprotobuf.ParseLENFieldBounds(prefix, off, tagLn, 42, protowire.VarintType)
		require.EqualError(t, err, "wrong type of field #42: expected LEN, got VARINT")
	})

	t.Run("invalid len", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				buf := slices.Concat(prefix, tc.buf)
				_, err := iprotobuf.ParseLENFieldBounds(buf, off, tagLn, 42, protowire.BytesType)
				require.ErrorContains(t, err, "parse field #42 of LEN type: parse varint")
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("buffer overflow", func(t *testing.T) {
		t.Run("int overflow", func(t *testing.T) {
			buf := slices.Concat(prefix, make([]byte, binary.MaxVarintLen64))
			n := binary.PutUvarint(buf[len(prefix):], uint64(math.MaxInt+1))

			_, err := iprotobuf.ParseLENFieldBounds(buf[:len(prefix)+n], off, tagLn, 42, protowire.BytesType)
			require.EqualError(t, err, "parse field #42 of LEN type: value "+strconv.FormatUint(math.MaxInt+1, 10)+" overflows int")
		})

		for i, tc := range varintTestcases {
			if tc.val == 0 || tc.val > 1<<20 {
				continue
			}

			buf := slices.Concat(prefix, tc.buf, make([]byte, tc.val-1))
			_, err := iprotobuf.ParseLENFieldBounds(buf, off, tagLn, 42, protowire.BytesType)
			require.EqualError(t, err, "parse field #42 of LEN type: unexpected EOF: need "+strconv.FormatUint(tc.val, 10)+" bytes, left "+strconv.FormatUint(tc.val-1, 10)+" in buffer", i)
		}
	})

	for i, tc := range varintTestcases {
		if tc.val > 1<<20 {
			continue
		}

		buf := make([]byte, tc.val)
		f, err := iprotobuf.ParseLENFieldBounds(slices.Concat(prefix, tc.buf, buf), off, tagLn, 42, protowire.BytesType)
		require.NoError(t, err, i)
		require.EqualValues(t, off, f.From)
		require.EqualValues(t, f.From+tagLn+tc.ln, f.ValueFrom)
		require.EqualValues(t, f.ValueFrom+int(tc.val), f.To)
	}
}

func TestParseEnum(t *testing.T) {
	t.Run("invalid varint", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := iprotobuf.ParseEnum[int32](tc.buf)
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("value overflow", func(t *testing.T) {
		_, _, err := iprotobuf.ParseEnum[int32](int32OverflowVarint)
		require.EqualError(t, err, "value 2147483648 overflows int32")
	})

	for i, tc := range varintTestcases {
		if tc.val > math.MaxInt32 {
			continue
		}

		u, n, err := iprotobuf.ParseEnum[int32](tc.buf)
		require.NoError(t, err, i)
		require.EqualValues(t, tc.val, u, i)
		require.EqualValues(t, tc.ln, n, i)
	}
}

func TestParseEnumField(t *testing.T) {
	t.Run("wrong type", func(t *testing.T) {
		_, _, err := iprotobuf.ParseEnumField[int32]([]byte{}, 42, protowire.BytesType)
		require.EqualError(t, err, "wrong type of field #42: expected VARINT, got LEN")
	})

	t.Run("invalid varint", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := iprotobuf.ParseEnumField[int32](tc.buf, 42, protowire.VarintType)
				require.ErrorContains(t, err, "parse field #42 of VARINT type: parse varint")
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("value overflow", func(t *testing.T) {
		_, _, err := iprotobuf.ParseEnumField[int32](int32OverflowVarint, 42, protowire.VarintType)
		require.EqualError(t, err, "parse field #42 of VARINT type: value 2147483648 overflows int32")
	})

	for i, tc := range varintTestcases {
		if tc.val > math.MaxInt32 {
			continue
		}

		u, n, err := iprotobuf.ParseEnumField[int32](tc.buf, 42, protowire.VarintType)
		require.NoError(t, err, i)
		require.EqualValues(t, tc.val, u, i)
		require.EqualValues(t, tc.ln, n, i)
	}
}

func TestParseUint32(t *testing.T) {
	t.Run("invalid varint", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := iprotobuf.ParseUint32(tc.buf)
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("value overflow", func(t *testing.T) {
		_, _, err := iprotobuf.ParseUint32(uint32OverflowVarint)
		require.EqualError(t, err, "value 4294967296 overflows uint32")
	})

	for i, tc := range varintTestcases {
		if tc.val > math.MaxUint32 {
			continue
		}

		u, n, err := iprotobuf.ParseUint32(tc.buf)
		require.NoError(t, err, i)
		require.EqualValues(t, tc.val, u, i)
		require.EqualValues(t, tc.ln, n, i)
	}
}

func TestParseUint32Field(t *testing.T) {
	t.Run("wrong type", func(t *testing.T) {
		_, _, err := iprotobuf.ParseUint32Field([]byte{}, 42, protowire.BytesType)
		require.EqualError(t, err, "wrong type of field #42: expected VARINT, got LEN")
	})

	t.Run("invalid varint", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := iprotobuf.ParseUint32Field(tc.buf, 42, protowire.VarintType)
				require.ErrorContains(t, err, "parse field #42 of VARINT type: parse varint")
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	t.Run("value overflow", func(t *testing.T) {
		_, _, err := iprotobuf.ParseUint32Field(uint32OverflowVarint, 42, protowire.VarintType)
		require.EqualError(t, err, "parse field #42 of VARINT type: value 4294967296 overflows uint32")
	})

	for i, tc := range varintTestcases {
		if tc.val > math.MaxUint32 {
			continue
		}

		u, n, err := iprotobuf.ParseUint32Field(tc.buf, 42, protowire.VarintType)
		require.NoError(t, err, i)
		require.EqualValues(t, tc.val, u, i)
		require.EqualValues(t, tc.ln, n, i)
	}
}

func TestParseUint64Field(t *testing.T) {
	t.Run("wrong type", func(t *testing.T) {
		_, _, err := iprotobuf.ParseUint64Field([]byte{}, 42, protowire.BytesType)
		require.EqualError(t, err, "wrong type of field #42: expected VARINT, got LEN")
	})

	t.Run("invalid varint", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := iprotobuf.ParseUint64Field(tc.buf, 42, protowire.VarintType)
				require.ErrorContains(t, err, "parse field #42 of VARINT type: parse varint")
				require.ErrorContains(t, err, tc.err)
			})
		}
	})

	for i, tc := range varintTestcases {
		if tc.val > math.MaxUint32 {
			continue
		}

		u, n, err := iprotobuf.ParseUint64Field(tc.buf, 42, protowire.VarintType)
		require.NoError(t, err, i)
		require.EqualValues(t, tc.val, u, i)
		require.EqualValues(t, tc.ln, n, i)
	}
}

func TestSkipField(t *testing.T) {
	t.Run("unknown type", func(t *testing.T) {
		_, err := iprotobuf.SkipField([]byte{}, 10, -1)
		require.EqualError(t, err, "unknown field type -1")

		_, err = iprotobuf.SkipField([]byte{}, 10, 6)
		require.EqualError(t, err, "unknown field type 6")
	})

	t.Run("VARINT", func(t *testing.T) {
		for _, tc := range invalidVarintTestcases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := iprotobuf.SkipField(tc.buf, 42, protowire.VarintType)
				require.ErrorContains(t, err, "parse field #42 of VARINT type")
				require.ErrorContains(t, err, tc.err)
			})
		}

		for i, tc := range varintTestcases {
			n, err := iprotobuf.SkipField(tc.buf, 42, protowire.VarintType)
			require.NoError(t, err, i)
			require.EqualValues(t, tc.ln, n, i)
		}
	})

	t.Run("I32", func(t *testing.T) {
		for ln := range 4 {
			_, err := iprotobuf.SkipField(make([]byte, ln), 42, protowire.Fixed32Type)
			require.EqualError(t, err, "parse field #42 of I32 type: unexpected EOF: need 4 bytes, left "+strconv.Itoa(ln)+" in buffer")
		}

		n, err := iprotobuf.SkipField(make([]byte, 4), 42, protowire.Fixed32Type)
		require.NoError(t, err)
		require.EqualValues(t, 4, n)
	})

	t.Run("I64", func(t *testing.T) {
		for ln := range 8 {
			_, err := iprotobuf.SkipField(make([]byte, ln), 42, protowire.Fixed64Type)
			require.EqualError(t, err, "parse field #42 of I64 type: unexpected EOF: need 8 bytes, left "+strconv.Itoa(ln)+" in buffer")
		}

		n, err := iprotobuf.SkipField(make([]byte, 8), 42, protowire.Fixed64Type)
		require.NoError(t, err)
		require.EqualValues(t, 8, n)
	})

	t.Run("LEN", func(t *testing.T) {
		t.Run("invalid len", func(t *testing.T) {
			for _, tc := range invalidVarintTestcases {
				t.Run(tc.name, func(t *testing.T) {
					_, err := iprotobuf.SkipField(tc.buf, 42, protowire.BytesType)
					require.ErrorContains(t, err, "parse field #42 of LEN type: parse varint")
					require.ErrorContains(t, err, tc.err)
				})
			}
		})

		t.Run("buffer overflow", func(t *testing.T) {
			t.Run("int overflow", func(t *testing.T) {
				buf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(buf, uint64(math.MaxInt+1))

				_, err := iprotobuf.SkipField(buf[:n], 42, protowire.BytesType)
				require.EqualError(t, err, "parse field #42 of LEN type: value "+strconv.FormatUint(math.MaxInt+1, 10)+" overflows int")
			})

			for i, tc := range varintTestcases {
				if tc.val == 0 || tc.val > 1<<20 {
					continue
				}

				buf := slices.Concat(tc.buf, make([]byte, tc.val-1))
				_, err := iprotobuf.SkipField(buf, 42, protowire.BytesType)
				require.EqualError(t, err, "parse field #42 of LEN type: unexpected EOF: need "+strconv.FormatUint(tc.val, 10)+" bytes, left "+strconv.FormatUint(tc.val-1, 10)+" in buffer", i)
			}
		})

		for i, tc := range varintTestcases {
			if tc.val > 1<<20 {
				continue
			}

			buf := make([]byte, tc.val)
			n, err := iprotobuf.SkipField(slices.Concat(tc.buf, buf), 42, protowire.BytesType)
			require.NoError(t, err, i)
			require.EqualValues(t, tc.ln+int(tc.val), n, i)
		}
	})

	t.Run("SGROUP", func(t *testing.T) {
		_, err := iprotobuf.SkipField([]byte{}, 42, protowire.StartGroupType)
		require.EqualError(t, err, "parse field #42 of SGROUP type: type is not supported")
	})

	t.Run("EGROUP", func(t *testing.T) {
		_, err := iprotobuf.SkipField([]byte{}, 42, protowire.EndGroupType)
		require.EqualError(t, err, "parse field #42 of EGROUP type: type is not supported")
	})
}
