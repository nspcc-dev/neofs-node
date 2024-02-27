package meta

import (
	"math"
	"math/rand"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/stretchr/testify/require"
)

func Test_getVarUint(t *testing.T) {
	data := make([]byte, 10)
	for _, val := range []uint64{0, 0xfc, 0xfd, 0xfffe, 0xffff, 0xfffffffe, 0xffffffff, math.MaxUint64} {
		expSize := io.PutVarUint(data, val)
		actual, actSize, err := getVarUint(data)
		require.NoError(t, err)
		require.Equal(t, val, actual)
		require.Equal(t, expSize, actSize, "value: %x", val)

		_, _, err = getVarUint(data[:expSize-1])
		require.Error(t, err)
	}
}

func Test_decodeList(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		lst, err := decodeList(nil)
		require.NoError(t, err)
		require.True(t, len(lst) == 0)
	})
	t.Run("empty, 0 len", func(t *testing.T) {
		lst, err := decodeList([]byte{0})
		require.NoError(t, err)
		require.True(t, len(lst) == 0)
	})
	t.Run("bad len", func(t *testing.T) {
		_, err := decodeList([]byte{0xfe})
		require.Error(t, err)
	})
	t.Run("random", func(t *testing.T) {
		expected := make([][]byte, 20)
		for i := range expected {
			expected[i] = make([]byte, rand.Uint32()%10)
			//nolint:staticcheck
			rand.Read(expected[i])
		}

		data, err := encodeList(expected)
		require.NoError(t, err)

		actual, err := decodeList(data)
		require.NoError(t, err)
		require.Equal(t, expected, actual)

		t.Run("unexpected EOF", func(t *testing.T) {
			for i := 1; i < len(data)-1; i++ {
				_, err := decodeList(data[:i])
				require.Error(t, err)
			}
		})
	})
}
