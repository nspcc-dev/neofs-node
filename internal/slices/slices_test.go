package slices_test

import (
	"testing"

	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	"github.com/stretchr/testify/require"
)

func TestTwoDimElementCount(t *testing.T) {
	require.Zero(t, islices.TwoDimSliceElementCount([][]int(nil)))
	require.Zero(t, islices.TwoDimSliceElementCount(make([][]int, 10)))
	require.EqualValues(t, 10, islices.TwoDimSliceElementCount([][]int{
		{1},
		{2, 3},
		{4, 5, 6},
		{7, 8, 9, 10},
	}))
}

func TestNilTwoDimSliceElements(t *testing.T) {
	require.Nil(t, islices.NilTwoDimSliceElements([][]int(nil), []int{1, 2, 3}))
	require.Empty(t, islices.NilTwoDimSliceElements([][]int{}, []int{1, 2, 3}))

	excl := []int{1, 3}
	res := islices.NilTwoDimSliceElements([][]int{
		{1},
		{2, 3},
		{4, 5, 6},
		{7, 8, 9, 10},
	}, excl)

	require.Equal(t, [][]int{
		{1},
		nil,
		{4, 5, 6},
		nil,
	}, res)
	require.EqualValues(t, len(excl), islices.CountNilsInTwoDimSlice(res))
}
