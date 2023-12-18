package deploy

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDivideFundsEvenly(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		var vals []uint64

		divideFundsEvenly(0, 5, func(ind int, amount uint64) {
			vals = append(vals, amount)
		})
		require.Empty(t, vals)
	})

	t.Run("less than N", func(t *testing.T) {
		var vals []uint64

		divideFundsEvenly(4, 5, func(ind int, amount uint64) {
			vals = append(vals, amount)
		})
		require.Len(t, vals, 4)
		for i := range vals {
			require.EqualValues(t, 1, vals[i])
		}
	})

	t.Run("multiple", func(t *testing.T) {
		var vals []uint64

		divideFundsEvenly(15, 3, func(ind int, amount uint64) {
			vals = append(vals, amount)
		})
		require.Len(t, vals, 3)
		for i := range vals {
			require.EqualValues(t, 5, vals[i])
		}
	})

	t.Run("with remainder", func(t *testing.T) {
		var vals []uint64

		divideFundsEvenly(16, 3, func(ind int, amount uint64) {
			vals = append(vals, amount)
		})
		require.Len(t, vals, 3)
		require.EqualValues(t, 6, vals[0])
		require.EqualValues(t, 5, vals[1])
		require.EqualValues(t, 5, vals[2])
	})
}

func BenchmarkDivideFundsEvenly(b *testing.B) {
	for _, tc := range []struct {
		n int
		a uint64
	}{
		{n: 7, a: 705},
		{n: 100, a: 100_000_000},
	} {
		b.Run(fmt.Sprintf("N=%d,amount=%d", tc.n, tc.a), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				divideFundsEvenly(tc.a, tc.n, func(ind int, amount uint64) {})
			}
		})
	}
}
