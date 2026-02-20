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

func TestSeekFieldByNumber(t *testing.T) {
	t.Run("invalid number", func(t *testing.T) {
		t.Run("0", func(t *testing.T) {
			for _, n := range []protowire.Number{-1, 0, 536870912} {
				_, _, _, err := iprotobuf.SeekFieldByNumber([]byte{}, n)
				require.EqualError(t, err, "invalid number "+strconv.Itoa(int(n)))
			}
		})
	})

	t.Run("empty buffer", func(t *testing.T) {
		off, _, _, err := iprotobuf.SeekFieldByNumber([]byte{}, 42)
		require.NoError(t, err)
		require.Negative(t, off)
	})

	// #1, VARINT, 1234567890
	fld1 := []byte{8, 210, 133, 216, 204, 4}
	// #100, I64, 2345678901
	fld2 := []byte{161, 6, 210, 56, 251, 13, 0, 0, 0, 0}
	// #5K, LEN, Hello, world!
	fld3 := []byte{194, 184, 2, 13, 72, 101, 108, 108, 111, 44, 32, 119, 111, 114, 108, 100, 33}
	// #1KK, I32, 3456789012
	fld4 := []byte{133, 164, 232, 3, 20, 106, 10, 206}

	t.Run("invalid tag", func(t *testing.T) {
		t.Run("invalid varint", func(t *testing.T) {
			for _, tc := range invalidVarintTestcases {
				if len(tc.buf) == 0 {
					continue
				}
				t.Run(tc.name, func(t *testing.T) {
					buf := slices.Concat(fld1, tc.buf)
					_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 42)
					require.ErrorContains(t, err, "parse tag at offset "+strconv.Itoa(len(fld1)))
					require.ErrorContains(t, err, "parse varint")
					require.ErrorContains(t, err, tc.err)
				})
			}
		})

		t.Run("invalid number", func(t *testing.T) {
			t.Run("0", func(t *testing.T) {
				buf := slices.Concat(fld1, []byte{2})
				_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 42) // 0,LEN
				require.EqualError(t, err, "parse tag at offset "+strconv.Itoa(len(fld1))+": invalid number 0")
			})
			t.Run("negative", func(t *testing.T) {
				buf := slices.Concat(fld1, []byte{250, 255, 255, 255, 255, 255, 255, 255, 255, 1}) // -1,LEN
				_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 42)
				require.EqualError(t, err, "parse tag at offset "+strconv.Itoa(len(fld1))+": invalid number -1")
			})
		})
	})

	t.Run("unordered fields", func(t *testing.T) {
		buf := slices.Concat(fld1, fld3, fld2)
		_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 5001)
		require.EqualError(t, err, "unordered fields: #100 after #5000")
	})

	t.Run("parse intermediate field failure", func(t *testing.T) {
		t.Run("varint", func(t *testing.T) {
			fld := slices.Concat([]byte{208, 2}, uint64OverflowVarint)
			buf := slices.Concat(fld1, fld, fld2, fld3, fld4)
			_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 5000)
			require.ErrorContains(t, err, "parse field #42 of VARINT type")
			require.ErrorContains(t, err, "variable length integer overflow")
		})
		t.Run("I64", func(t *testing.T) {
			buf := slices.Concat(fld1, fld2[:len(fld2)-1])
			_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 5000)
			require.EqualError(t, err, "parse field #100 of I64 type: unexpected EOF: need 8 bytes, left 7 in buffer")
		})
		t.Run("LEN", func(t *testing.T) {
			tag := []byte{210, 2}
			t.Run("invalid len", func(t *testing.T) {
				for _, tc := range invalidVarintTestcases {
					t.Run(tc.name, func(t *testing.T) {
						buf := slices.Concat(fld1, tag, tc.buf)
						_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 43)
						require.ErrorContains(t, err, "parse field #42 of LEN type")
						require.ErrorContains(t, err, "parse varint")
						require.ErrorContains(t, err, tc.err)
					})
				}
			})
			t.Run("buffer overflow", func(t *testing.T) {
				t.Run("int overflow", func(t *testing.T) {
					buf := make([]byte, binary.MaxVarintLen64)
					n := binary.PutUvarint(buf, uint64(math.MaxInt+1))

					buf = slices.Concat(fld1, tag, buf[:n])

					_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 43)
					require.EqualError(t, err, "parse field #42 of LEN type: value "+strconv.FormatUint(math.MaxInt+1, 10)+" overflows int")
				})

				for i, tc := range varintTestcases {
					if tc.val == 0 || tc.val > 1<<20 {
						continue
					}

					buf := slices.Concat(tc.buf, make([]byte, tc.val-1))
					buf = slices.Concat(fld1, tag, buf)
					_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 43)
					require.EqualError(t, err, "parse field #42 of LEN type: unexpected EOF: need "+strconv.FormatUint(tc.val, 10)+" bytes, left "+strconv.FormatUint(tc.val-1, 10)+" in buffer", i)
				}
			})
		})
		t.Run("SGROUP", func(t *testing.T) {
			buf := slices.Concat(fld1, []byte{211, 2})
			_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 43)
			require.EqualError(t, err, "parse field #42 of SGROUP type: type is not supported")
		})
		t.Run("EGROUP", func(t *testing.T) {
			buf := slices.Concat(fld1, []byte{212, 2})
			_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 43)
			require.EqualError(t, err, "parse field #42 of EGROUP type: type is not supported")
		})
		t.Run("I32", func(t *testing.T) {
			buf := slices.Concat(fld1, fld4[:len(fld4)-1])
			_, _, _, err := iprotobuf.SeekFieldByNumber(buf, 1_000_001)
			require.EqualError(t, err, "parse field #1000000 of I32 type: unexpected EOF: need 4 bytes, left 3 in buffer")
		})
	})

	message := slices.Concat(fld1, fld2, fld3, fld4)

	t.Run("missing", func(t *testing.T) {
		for _, n := range []protowire.Number{2, 99, 101, 4999, 50001, 999_999, 1_000_001} {
			off, _, _, err := iprotobuf.SeekFieldByNumber(message, n)
			require.NoError(t, err, n)
			require.Negative(t, off, n)
		}
	})

	off, tagLn, typ, err := iprotobuf.SeekFieldByNumber(message, 1)
	require.NoError(t, err)
	require.EqualValues(t, 0, off)
	require.EqualValues(t, 1, tagLn)
	require.EqualValues(t, protowire.VarintType, typ)

	off, tagLn, typ, err = iprotobuf.SeekFieldByNumber(message, 100)
	require.NoError(t, err)
	require.EqualValues(t, len(fld1), off)
	require.EqualValues(t, 2, tagLn)
	require.EqualValues(t, protowire.Fixed64Type, typ)

	off, tagLn, typ, err = iprotobuf.SeekFieldByNumber(message, 5_000)
	require.NoError(t, err)
	require.EqualValues(t, len(fld1)+len(fld2), off)
	require.EqualValues(t, 3, tagLn)
	require.EqualValues(t, protowire.BytesType, typ)

	off, tagLn, typ, err = iprotobuf.SeekFieldByNumber(message, 1_000_000)
	require.NoError(t, err)
	require.EqualValues(t, len(fld1)+len(fld2)+len(fld3), off)
	require.EqualValues(t, 4, tagLn)
	require.EqualValues(t, protowire.Fixed32Type, typ)
}

func TestGetLENFieldBounds(t *testing.T) {
	t.Run("invalid number", func(t *testing.T) {
		t.Run("0", func(t *testing.T) {
			for _, n := range []protowire.Number{-1, 0, 536870912} {
				_, err := iprotobuf.GetLENFieldBounds([]byte{}, n)
				require.EqualError(t, err, "invalid number "+strconv.Itoa(int(n)))
			}
		})
	})

	t.Run("empty buffer", func(t *testing.T) {
		f, err := iprotobuf.GetLENFieldBounds([]byte{}, 42)
		require.NoError(t, err)
		require.True(t, f.IsMissing())
	})

	// #1, VARINT, 1234567890
	fld1 := []byte{8, 210, 133, 216, 204, 4}
	// #100, I64, 2345678901
	fld2 := []byte{161, 6, 210, 56, 251, 13, 0, 0, 0, 0}
	// #5K, LEN, Hello, world!
	fld3 := []byte{194, 184, 2, 13, 72, 101, 108, 108, 111, 44, 32, 119, 111, 114, 108, 100, 33}
	// #1KK, I32, 3456789012
	fld4 := []byte{133, 164, 232, 3, 20, 106, 10, 206}

	t.Run("seek failure", func(t *testing.T) {
		t.Run("invalid tag", func(t *testing.T) {
			t.Run("invalid varint", func(t *testing.T) {
				for _, tc := range invalidVarintTestcases {
					if len(tc.buf) == 0 {
						continue
					}
					t.Run(tc.name, func(t *testing.T) {
						buf := slices.Concat(fld1, tc.buf)
						_, err := iprotobuf.GetLENFieldBounds(buf, 42)
						require.ErrorContains(t, err, "parse tag at offset "+strconv.Itoa(len(fld1)))
						require.ErrorContains(t, err, "parse varint")
						require.ErrorContains(t, err, tc.err)
					})
				}
			})

			t.Run("invalid number", func(t *testing.T) {
				t.Run("0", func(t *testing.T) {
					buf := slices.Concat(fld1, []byte{2})
					_, err := iprotobuf.GetLENFieldBounds(buf, 42) // 0,LEN
					require.EqualError(t, err, "parse tag at offset "+strconv.Itoa(len(fld1))+": invalid number 0")
				})
				t.Run("negative", func(t *testing.T) {
					buf := slices.Concat(fld1, []byte{250, 255, 255, 255, 255, 255, 255, 255, 255, 1}) // -1,LEN
					_, err := iprotobuf.GetLENFieldBounds(buf, 42)
					require.EqualError(t, err, "parse tag at offset "+strconv.Itoa(len(fld1))+": invalid number -1")
				})
			})
		})

		t.Run("unordered fields", func(t *testing.T) {
			buf := slices.Concat(fld1, fld3, fld2)
			_, err := iprotobuf.GetLENFieldBounds(buf, 5001)
			require.EqualError(t, err, "unordered fields: #100 after #5000")
		})

		t.Run("parse intermediate field failure", func(t *testing.T) {
			t.Run("varint", func(t *testing.T) {
				fld := slices.Concat([]byte{208, 2}, uint64OverflowVarint)
				buf := slices.Concat(fld1, fld, fld2, fld3, fld4)
				_, err := iprotobuf.GetLENFieldBounds(buf, 5000)
				require.ErrorContains(t, err, "parse field #42 of VARINT type")
				require.ErrorContains(t, err, "variable length integer overflow")
			})
			t.Run("I64", func(t *testing.T) {
				buf := slices.Concat(fld1, fld2[:len(fld2)-1])
				_, err := iprotobuf.GetLENFieldBounds(buf, 5000)
				require.EqualError(t, err, "parse field #100 of I64 type: unexpected EOF: need 8 bytes, left 7 in buffer")
			})
			t.Run("LEN", func(t *testing.T) {
				tag := []byte{210, 2}
				t.Run("invalid len", func(t *testing.T) {
					for _, tc := range invalidVarintTestcases {
						t.Run(tc.name, func(t *testing.T) {
							buf := slices.Concat(fld1, tag, tc.buf)
							_, err := iprotobuf.GetLENFieldBounds(buf, 43)
							require.ErrorContains(t, err, "parse field #42 of LEN type")
							require.ErrorContains(t, err, "parse varint")
							require.ErrorContains(t, err, tc.err)
						})
					}
				})
				t.Run("buffer overflow", func(t *testing.T) {
					t.Run("int overflow", func(t *testing.T) {
						buf := make([]byte, binary.MaxVarintLen64)
						n := binary.PutUvarint(buf, uint64(math.MaxInt+1))

						buf = slices.Concat(fld1, tag, buf[:n])

						_, err := iprotobuf.GetLENFieldBounds(buf, 43)
						require.EqualError(t, err, "parse field #42 of LEN type: value "+strconv.FormatUint(math.MaxInt+1, 10)+" overflows int")
					})

					for i, tc := range varintTestcases {
						if tc.val == 0 || tc.val > 1<<20 {
							continue
						}

						buf := slices.Concat(tc.buf, make([]byte, tc.val-1))
						buf = slices.Concat(fld1, tag, buf)
						_, err := iprotobuf.GetLENFieldBounds(buf, 43)
						require.EqualError(t, err, "parse field #42 of LEN type: unexpected EOF: need "+strconv.FormatUint(tc.val, 10)+" bytes, left "+strconv.FormatUint(tc.val-1, 10)+" in buffer", i)
					}
				})
			})
			t.Run("SGROUP", func(t *testing.T) {
				buf := slices.Concat(fld1, []byte{211, 2})
				_, err := iprotobuf.GetLENFieldBounds(buf, 43)
				require.EqualError(t, err, "parse field #42 of SGROUP type: type is not supported")
			})
			t.Run("EGROUP", func(t *testing.T) {
				buf := slices.Concat(fld1, []byte{212, 2})
				_, err := iprotobuf.GetLENFieldBounds(buf, 43)
				require.EqualError(t, err, "parse field #42 of EGROUP type: type is not supported")
			})
			t.Run("I32", func(t *testing.T) {
				buf := slices.Concat(fld1, fld4[:len(fld4)-1])
				_, err := iprotobuf.GetLENFieldBounds(buf, 1_000_001)
				require.EqualError(t, err, "parse field #1000000 of I32 type: unexpected EOF: need 4 bytes, left 3 in buffer")
			})
		})
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := iprotobuf.GetLENFieldBounds([]byte{208, 2}, 42)
		require.EqualError(t, err, "wrong type of field #42: expected LEN, got VARINT")
	})

	t.Run("parse failure", func(t *testing.T) {
		tag := []byte{210, 2}
		t.Run("invalid len", func(t *testing.T) {
			for _, tc := range invalidVarintTestcases {
				t.Run(tc.name, func(t *testing.T) {
					buf := slices.Concat(fld1, tag, tc.buf)
					_, err := iprotobuf.GetLENFieldBounds(buf, 42)
					require.ErrorContains(t, err, "parse field #42 of LEN type")
					require.ErrorContains(t, err, "parse varint")
					require.ErrorContains(t, err, tc.err)
				})
			}
		})
		t.Run("buffer overflow", func(t *testing.T) {
			t.Run("int overflow", func(t *testing.T) {
				buf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(buf, uint64(math.MaxInt+1))

				buf = slices.Concat(fld1, tag, buf[:n])

				_, err := iprotobuf.GetLENFieldBounds(buf, 42)
				require.EqualError(t, err, "parse field #42 of LEN type: value "+strconv.FormatUint(math.MaxInt+1, 10)+" overflows int")
			})

			for i, tc := range varintTestcases {
				if tc.val == 0 || tc.val > 1<<20 {
					continue
				}

				buf := slices.Concat(tc.buf, make([]byte, tc.val-1))
				buf = slices.Concat(fld1, tag, buf)
				_, err := iprotobuf.GetLENFieldBounds(buf, 42)
				require.EqualError(t, err, "parse field #42 of LEN type: unexpected EOF: need "+strconv.FormatUint(tc.val, 10)+" bytes, left "+strconv.FormatUint(tc.val-1, 10)+" in buffer", i)
			}
		})
	})

	message := slices.Concat(fld1, fld2, fld3, fld4)

	t.Run("missing", func(t *testing.T) {
		for _, n := range []protowire.Number{2, 99, 101, 4999, 50001, 999_999, 1_000_001} {
			f, err := iprotobuf.GetLENFieldBounds(message, n)
			require.NoError(t, err, n)
			require.True(t, f.IsMissing())
		}
	})

	f, err := iprotobuf.GetLENFieldBounds(fld3, 5000)
	require.NoError(t, err)
	require.False(t, f.IsMissing())
	require.EqualValues(t, 0, f.From)
	require.EqualValues(t, 4, f.ValueFrom)
	require.EqualValues(t, len(fld3), f.To)

	f, err = iprotobuf.GetLENFieldBounds(slices.Concat(fld1, fld2, fld3), 5000)
	require.NoError(t, err)
	require.False(t, f.IsMissing())
	require.EqualValues(t, len(fld1)+len(fld2), f.From)
	require.EqualValues(t, f.From+4, f.ValueFrom)
	require.EqualValues(t, f.From+len(fld3), f.To)

	f, err = iprotobuf.GetLENFieldBounds(message, 5_000)
	require.NoError(t, err)
	require.False(t, f.IsMissing())
	require.EqualValues(t, len(fld1)+len(fld2), f.From)
	require.EqualValues(t, f.From+4, f.ValueFrom)
	require.EqualValues(t, f.From+len(fld3), f.To)
}
