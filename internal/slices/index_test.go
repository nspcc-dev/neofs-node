package slices_test

import (
	"slices"
	"testing"

	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/stretchr/testify/require"
)

func TestIndexes(t *testing.T) {
	require.Empty(t, islices.Indexes(0))
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, islices.Indexes(10))
}

func TestIndexCombos(t *testing.T) {
	require.ElementsMatch(t, islices.IndexCombos(4, 2), [][]int{
		{0, 1},
		{0, 2},
		{0, 3},
		{1, 2},
		{1, 3},
		{2, 3},
	})
}

func TestCollectIndex(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Empty(t, islices.CollectIndex([]any(nil)))
	})
	t.Run("empty", func(t *testing.T) {
		require.Empty(t, islices.CollectIndex([]any{}))
	})

	s := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	sc := slices.Clone(s)

	newS := islices.CollectIndex(s, 9, 7, 5, 3, 1)
	require.Equal(t, []string{"9", "7", "5", "3", "1"}, newS)
	require.Len(t, newS, 5)
	require.EqualValues(t, 5, cap(newS))

	newS[0] += "_"
	require.Equal(t, sc, s)
}
