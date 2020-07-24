package epoch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEpochRelations(t *testing.T) {
	items := []struct {
		relFn func(Epoch, Epoch) bool

		base, ok, fail uint64
	}{
		{relFn: EQ, base: 1, ok: 1, fail: 2},
		{relFn: NE, base: 1, ok: 2, fail: 1},
		{relFn: LT, base: 1, ok: 2, fail: 0},
		{relFn: GT, base: 1, ok: 0, fail: 2},
		{relFn: LE, base: 1, ok: 1, fail: 0},
		{relFn: LE, base: 1, ok: 2, fail: 0},
		{relFn: GE, base: 1, ok: 0, fail: 2},
		{relFn: GE, base: 1, ok: 1, fail: 2},
	}

	for _, item := range items {
		require.True(t,
			item.relFn(
				FromUint64(item.base),
				FromUint64(item.ok),
			),
		)

		require.False(t,
			item.relFn(
				FromUint64(item.base),
				FromUint64(item.fail),
			),
		)
	}
}
