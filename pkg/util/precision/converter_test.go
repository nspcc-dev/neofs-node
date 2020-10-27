package precision_test

import (
	"math/big"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
	"github.com/stretchr/testify/require"
)

func TestFixed8Converter_ToBalancePrecision(t *testing.T) {
	var n int64 = 100*100_000_000 + 12_345_678

	t.Run("same precision", func(t *testing.T) {
		cnv := precision.NewConverter(8)
		res := cnv.ToBalancePrecision(n)
		require.Equal(t, n, res)
	})

	t.Run("bigger target", func(t *testing.T) {
		cnv := precision.NewConverter(10)
		exp := n * 100
		res := cnv.ToBalancePrecision(n)
		require.Equal(t, exp, res)
	})

	t.Run("less target", func(t *testing.T) {
		cnv := precision.NewConverter(6)
		exp := n / 100
		res := cnv.ToBalancePrecision(n)
		require.Equal(t, exp, res)

		cnv = precision.NewConverter(0)
		exp = n / 100_000_000
		res = cnv.ToBalancePrecision(n)
		require.Equal(t, exp, res)
	})
}

func TestFixed8Converter_ToFixed8(t *testing.T) {
	var n int64 = 100*10_000_000 + 12_345_678

	t.Run("same precision", func(t *testing.T) {
		cnv := precision.NewConverter(8)
		res := cnv.ToFixed8(n)
		require.Equal(t, n, res)
	})

	t.Run("bigger target", func(t *testing.T) {
		cnv := precision.NewConverter(10)
		exp := n / 100
		res := cnv.ToFixed8(n)
		require.Equal(t, exp, res)
	})

	t.Run("less target", func(t *testing.T) {
		cnv := precision.NewConverter(6)
		exp := n * 100
		res := cnv.ToFixed8(n)
		require.Equal(t, exp, res)

		n = 1
		cnv = precision.NewConverter(0)
		exp = n * 100_000_000
		res = cnv.ToFixed8(n)
		require.Equal(t, exp, res)
	})
}

func TestConvert(t *testing.T) {
	n := big.NewInt(100*10_000_000 + 12_345_678)

	t.Run("same precision", func(t *testing.T) {
		require.Equal(t, n, precision.Convert(8, 8, n))
		require.Equal(t, n, precision.Convert(0, 0, n))
	})

	t.Run("bigger target", func(t *testing.T) {
		exp := new(big.Int).Mul(n, big.NewInt(100))
		require.Equal(t, exp, precision.Convert(8, 10, n))
		require.Equal(t, exp, precision.Convert(0, 2, n))
	})

	t.Run("less target", func(t *testing.T) {
		exp := new(big.Int).Div(n, big.NewInt(100))
		require.Equal(t, exp, precision.Convert(10, 8, n))
		require.Equal(t, exp, precision.Convert(2, 0, n))
	})
}
