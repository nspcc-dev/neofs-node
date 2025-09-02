package slices_test

import (
	"errors"
	"slices"
	"strconv"
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

func TestAllZeros(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.True(t, islices.AllZeros(nil))
	})
	t.Run("empty", func(t *testing.T) {
		require.True(t, islices.AllZeros([]byte{}))
	})

	s := make([]byte, 32)
	require.True(t, islices.AllZeros(s))

	for i := range s {
		sc := slices.Clone(s)
		sc[i]++
		require.False(t, islices.AllZeros(sc), i)
	}
}

func TestRepeatElements(t *testing.T) {
	tcs := []struct {
		name string
		e    any
	}{
		{name: "int", e: 1},
		{name: "bool", e: true},
		{name: "error", e: errors.New("some error")},
		{name: "nil", e: nil},
		{name: "struct", e: struct {
			i int
			s string
		}{
			i: 1,
			s: "foo",
		}},
		{name: "slice", e: []string{"foo", "bar"}},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			for _, n := range []int{0, 1, 10} {
				t.Run(strconv.Itoa(n), func(t *testing.T) {
					s := islices.RepeatElement(n, tc.e)
					require.Len(t, s, n)
					for i := range s {
						require.Equal(t, tc.e, s[i])
					}
				})
			}
		})
	}
}
