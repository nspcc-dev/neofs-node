package epoch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEpochMath(t *testing.T) {
	items := []struct {
		mathFn func(Epoch, Epoch) Epoch

		a, b, c uint64
	}{
		{
			mathFn: Sum, a: 1, b: 2, c: 3},
	}

	for _, item := range items {
		require.Equal(t,
			item.mathFn(
				FromUint64(item.a),
				FromUint64(item.b),
			),
			FromUint64(item.c),
		)
	}
}
