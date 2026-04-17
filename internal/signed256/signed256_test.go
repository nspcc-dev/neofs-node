package signed256

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDecimal(t *testing.T) {
	maxDec := Max().String()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "zero", in: "0", want: "0"},
		{name: "leading plus", in: "+42", want: "42"},
		{name: "leading zeros", in: "00042", want: "42"},
		{name: "negative", in: "-42", want: "-42"},
		{name: "negative zero", in: "-0", want: "0"},
		{name: "max", in: maxDec, want: maxDec},
		{name: "min", in: "-" + maxDec, want: "-" + maxDec},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDecimal(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.String())
		})
	}

	t.Run("errors", func(t *testing.T) {
		testErrs := []string{
			"",
			"+",
			"-",
			"abc",
			"1a",
			"-1a",
			"115792089237316195423570985008687907853269984665640564039457584007913129639936",
			"-115792089237316195423570985008687907853269984665640564039457584007913129639936",
		}

		for _, tc := range testErrs {
			t.Run(tc, func(t *testing.T) {
				_, err := ParseDecimal(tc)
				require.Error(t, err)
			})
		}
	})
}

func TestParseNormalizedDecimal(t *testing.T) {
	maxDec := Max().String()

	tests := []struct {
		name   string
		neg    bool
		digits string
		want   string
	}{
		{name: "zero", digits: "0", want: "0"},
		{name: "zero fast path", digits: "00000000000000000000", want: "0"},
		{name: "positive", digits: "42", want: "42"},
		{name: "positive fast path max uint64", digits: "18446744073709551615", want: "18446744073709551615"},
		{name: "negative", neg: true, digits: "42", want: "-42"},
		{name: "negative fast path max uint64", neg: true, digits: "18446744073709551615", want: "-18446744073709551615"},
		{name: "negative zero", neg: true, digits: "0", want: "0"},
		{name: "leading zeros", digits: "00042", want: "42"},
		{name: "max", digits: maxDec, want: maxDec},
		{name: "min", neg: true, digits: maxDec, want: "-" + maxDec},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseNormalizedDecimal(tc.neg, tc.digits)
			require.NoError(t, err)
			require.Equal(t, tc.want, got.String())
		})
	}

	t.Run("errors", func(t *testing.T) {
		testErrs := []struct {
			name   string
			neg    bool
			digits string
		}{
			{name: "empty"},
			{name: "non decimal", digits: "1a"},
			{name: "with plus", digits: "+1"},
			{name: "with minus", digits: "-1"},
			{name: "out of range", digits: "115792089237316195423570985008687907853269984665640564039457584007913129639936"},
		}

		for _, tc := range testErrs {
			t.Run(tc.name, func(t *testing.T) {
				_, err := ParseNormalizedDecimal(tc.neg, tc.digits)
				require.Error(t, err)
			})
		}
	})
}

func TestSetFromDecimal(t *testing.T) {
	maxDec := Max().String()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "zero resets previous sign", in: "0", want: "0"},
		{name: "positive", in: "42", want: "42"},
		{name: "leading plus", in: "+42", want: "42"},
		{name: "leading zeros", in: "00042", want: "42"},
		{name: "negative", in: "-42", want: "-42"},
		{name: "negative zero", in: "-0", want: "0"},
		{name: "max", in: maxDec, want: maxDec},
		{name: "min", in: "-" + maxDec, want: "-" + maxDec},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			z := MustParseDecimal("-1")
			err := z.SetFromDecimal(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, z.String())
		})
	}

	t.Run("error clears previous value", func(t *testing.T) {
		z := MustParseDecimal("-42")
		err := z.SetFromDecimal("")
		require.Error(t, err)
		require.Equal(t, "0", z.String())
	})

	t.Run("errors", func(t *testing.T) {
		testErrs := []string{
			"",
			"+",
			"-",
			"abc",
			"1a",
			"-1a",
			"115792089237316195423570985008687907853269984665640564039457584007913129639936",
			"-115792089237316195423570985008687907853269984665640564039457584007913129639936",
		}

		for _, tc := range testErrs {
			t.Run(tc, func(t *testing.T) {
				var z Int
				err := z.SetFromDecimal(tc)
				require.Error(t, err)
			})
		}
	})
}

func TestNewInt(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want string
	}{
		{name: "min int64", in: -1 << 63, want: "-9223372036854775808"},
		{name: "negative one", in: -1, want: "-1"},
		{name: "zero", in: 0, want: "0"},
		{name: "one", in: 1, want: "1"},
		{name: "max int64", in: 1<<63 - 1, want: "9223372036854775807"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, NewInt(tc.in).String())
		})
	}
}

func TestNewUint64(t *testing.T) {
	tests := []struct {
		name string
		in   uint64
		want string
	}{
		{name: "zero", in: 0, want: "0"},
		{name: "one", in: 1, want: "1"},
		{name: "max uint64", in: ^uint64(0), want: "18446744073709551615"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, NewUint64(tc.in).String())
		})
	}
}

func TestAdd(t *testing.T) {
	maxDec := Max().String()

	tests := []struct {
		name string
		x    string
		y    string
		want string
	}{
		{name: "zero", x: "0", y: "0", want: "0"},
		{name: "positive", x: "1", y: "2", want: "3"},
		{name: "negative", x: "-1", y: "-2", want: "-3"},
		{name: "cross sign positive", x: "10", y: "-3", want: "7"},
		{name: "cross sign negative", x: "-10", y: "3", want: "-7"},
		{name: "cancel", x: "-10", y: "10", want: "0"},
		{name: "min plus one", x: "-" + maxDec, y: "1", want: "-115792089237316195423570985008687907853269984665640564039457584007913129639934"},
		{name: "max minus one", x: maxDec, y: "-1", want: "115792089237316195423570985008687907853269984665640564039457584007913129639934"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			x := MustParseDecimal(tc.x)
			y := MustParseDecimal(tc.y)
			var z Int
			err := z.Add(&x, &y)
			require.NoError(t, err)
			require.Equal(t, tc.want, z.String())
		})
	}

	t.Run("overflow", func(t *testing.T) {
		maxSigned := Max()
		one := NewInt(1)
		var z Int
		err := z.Add(&maxSigned, &one)
		require.Error(t, err)
		require.Equal(t, "0", z.String())
	})

	t.Run("underflow", func(t *testing.T) {
		minSigned := Min()
		minusOne := NewInt(-1)
		var z Int
		err := z.Add(&minSigned, &minusOne)
		require.Error(t, err)
		require.Equal(t, "0", z.String())
	})
}

func TestCmp(t *testing.T) {
	maxDec := Max().String()

	tests := []struct {
		name string
		a    string
		b    string
		want int
	}{
		{name: "equal", a: "0", b: "0", want: 0},
		{name: "negative less", a: "-1", b: "0", want: -1},
		{name: "positive more", a: "1", b: "0", want: 1},
		{name: "negative by magnitude", a: "-2", b: "-1", want: -1},
		{name: "positive by magnitude", a: "2", b: "11", want: -1},
		{name: "min less max", a: "-" + maxDec, b: maxDec, want: -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := MustParseDecimal(tc.a)
			b := MustParseDecimal(tc.b)
			require.Equal(t, tc.want, a.Cmp(&b))
			require.Equal(t, -tc.want, b.Cmp(&a))
		})
	}
}

func TestFillBytes(t *testing.T) {
	maxDec := Max().String()

	tests := []string{
		"0",
		"1",
		"-1",
		"18446744073709551615",
		"-18446744073709551615",
		maxDec,
		"-" + maxDec,
	}

	for _, tc := range tests {
		t.Run(tc, func(t *testing.T) {
			v := MustParseDecimal(tc)

			var got [EncodedLen]byte
			v.FillBytes(got[:])

			want := v.EncodeBytes()
			require.Equal(t, want, got)

			dec, err := DecodeBytes(got[:])
			require.NoError(t, err)
			require.Equal(t, v, dec)
		})
	}

	t.Run("short buffer panics", func(t *testing.T) {
		v := NewInt(1)
		require.Panics(t, func() {
			v.FillBytes(make([]byte, EncodedLen-1))
		})
	})
}

func TestEncodeDecodeMeta(t *testing.T) {
	maxDec := Max().String()

	tests := []string{
		"0",
		"1",
		"-1",
		"18446744073709551615",
		"-18446744073709551615",
		maxDec,
		"-" + maxDec,
	}

	for _, tc := range tests {
		t.Run(tc, func(t *testing.T) {
			v := MustParseDecimal(tc)
			enc := v.EncodeBytes()
			dec, err := DecodeBytes(enc[:])
			require.NoError(t, err)
			require.Equal(t, v, dec)
		})
	}

	_, err := DecodeBytes([]byte{1})
	require.Error(t, err)

	var b [EncodedLen]byte
	b[0] = 2
	_, err = DecodeBytes(b[:])
	require.Error(t, err)
}

func TestEncodeMetaOrderingMatchesCmp(t *testing.T) {
	maxDec := Max().String()

	values := []Int{
		MustParseDecimal("-" + maxDec),
		MustParseDecimal("-18446744073709551616"),
		MustParseDecimal("-18446744073709551615"),
		MustParseDecimal("-2"),
		NewInt(-1),
		NewInt(0),
		NewInt(1),
		MustParseDecimal("2"),
		MustParseDecimal("18446744073709551615"),
		MustParseDecimal("18446744073709551616"),
		MustParseDecimal(maxDec),
	}

	for i := range values {
		for j := range values {
			gotCmp := values[i].Cmp(&values[j])
			left := values[i].EncodeBytes()
			right := values[j].EncodeBytes()
			gotBytesCmp := bytes.Compare(left[:], right[:])
			switch {
			case gotCmp < 0 && gotBytesCmp >= 0:
				t.Fatalf("expected bytes(%q) < bytes(%q), got %d", values[i].String(), values[j].String(), gotBytesCmp)
			case gotCmp == 0 && gotBytesCmp != 0:
				t.Fatalf("expected bytes(%q) == bytes(%q), got %d", values[i].String(), values[j].String(), gotBytesCmp)
			case gotCmp > 0 && gotBytesCmp <= 0:
				t.Fatalf("expected bytes(%q) > bytes(%q), got %d", values[i].String(), values[j].String(), gotBytesCmp)
			}
		}
	}
}

func TestCompatibilityWithBigIntMetaEncoding(t *testing.T) {
	maxDec := Max().String()

	values := []string{
		"-" + maxDec,
		"-18446744073709551616",
		"-18446744073709551615",
		"-2",
		"-1",
		"-0",
		"0",
		"1",
		"2",
		"18446744073709551615",
		"18446744073709551616",
		maxDec,
	}

	for _, tc := range values {
		t.Run(tc, func(t *testing.T) {
			v := MustParseDecimal(tc)
			got := v.EncodeBytes()
			want := oldBigIntMetaEncoding(t, tc)
			require.Equal(t, want, got)

			decoded, err := DecodeBytes(want[:])
			require.NoError(t, err)

			canonical := new(big.Int)
			_, ok := canonical.SetString(tc, 10)
			require.True(t, ok)
			require.Equal(t, canonical.String(), decoded.String())
		})
	}
}

func oldBigIntMetaEncoding(t *testing.T, s string) [EncodedLen]byte {
	t.Helper()

	var out [EncodedLen]byte
	n, ok := new(big.Int).SetString(s, 10)
	require.True(t, ok)

	neg := n.Sign() < 0
	if neg {
		out[0] = 0
	} else {
		out[0] = 1
	}
	n.FillBytes(out[1:])
	if neg {
		for i := range out[1:] {
			out[1+i] = ^out[1+i]
		}
	}
	return out
}
