package client

import (
	"math"
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/encoding/bigint"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/stretchr/testify/require"
)

var (
	bigIntValue = new(big.Int).Mul(big.NewInt(math.MaxInt64), big.NewInt(10))

	stringByteItem     = stackitem.NewByteArray([]byte("Hello World"))
	intItem            = stackitem.NewBigInteger(new(big.Int).SetInt64(1))
	bigIntItem         = stackitem.NewBigInteger(bigIntValue)
	byteWithIntItem    = stackitem.NewByteArray([]byte{0x0a})
	byteWithBigIntItem = stackitem.NewByteArray(bigint.ToBytes(bigIntValue))
	emptyByteArrayItem = stackitem.NewByteArray([]byte{})
	trueBoolItem       = stackitem.NewBool(true)
	falseBoolItem      = stackitem.NewBool(false)
	arrayItem          = stackitem.NewArray([]stackitem.Item{intItem, stringByteItem})
	anyTypeItem        = stackitem.Null{}
)

func TestBoolFromStackItem(t *testing.T) {
	t.Run("true assert", func(t *testing.T) {
		val, err := BoolFromStackItem(trueBoolItem)
		require.NoError(t, err)
		require.True(t, val)

		val, err = BoolFromStackItem(intItem)
		require.NoError(t, err)
		require.True(t, val)
	})

	t.Run("false assert", func(t *testing.T) {
		val, err := BoolFromStackItem(falseBoolItem)
		require.NoError(t, err)
		require.False(t, val)

		val, err = BoolFromStackItem(emptyByteArrayItem)
		require.NoError(t, err)
		require.False(t, val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := BoolFromStackItem(arrayItem)
		require.Error(t, err)
	})
}

func TestArrayFromStackItem(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := ArrayFromStackItem(arrayItem)
		require.NoError(t, err)
		require.Len(t, val, len(arrayItem.Value().([]stackitem.Item)))
	})
	t.Run("incorrect assert", func(t *testing.T) {
		_, err := ArrayFromStackItem(stringByteItem)
		require.Error(t, err)
	})
	t.Run("nil array case", func(t *testing.T) {
		val, err := ArrayFromStackItem(anyTypeItem)
		require.NoError(t, err)
		require.Nil(t, val)
	})
}

func TestBytesFromStackItem(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := BytesFromStackItem(stringByteItem)
		require.NoError(t, err)
		require.Equal(t, stringByteItem.Value().([]byte), val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := BytesFromStackItem(arrayItem)
		require.Error(t, err)
	})
}

func TestIntFromStackItem(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := IntFromStackItem(intItem)
		require.NoError(t, err)
		require.Equal(t, intItem.Value().(*big.Int).Int64(), val)

		val, err = IntFromStackItem(byteWithIntItem)
		require.NoError(t, err)
		require.Equal(t, int64(0x0a), val)

		val, err = IntFromStackItem(emptyByteArrayItem)
		require.NoError(t, err)
		require.Equal(t, int64(0), val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := IntFromStackItem(arrayItem)
		require.Error(t, err)
	})
}

func TestBigIntFromStackItem(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := BigIntFromStackItem(bigIntItem)
		require.NoError(t, err)
		require.Equal(t, bigIntValue, val)

		val, err = BigIntFromStackItem(byteWithBigIntItem)
		require.NoError(t, err)
		require.Equal(t, bigIntValue, val)

		val, err = BigIntFromStackItem(emptyByteArrayItem)
		require.NoError(t, err)
		require.Equal(t, big.NewInt(0), val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := BigIntFromStackItem(arrayItem)
		require.Error(t, err)
	})
}

func TestStringFromStackItem(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := StringFromStackItem(stringByteItem)
		require.NoError(t, err)
		require.Equal(t, string(stringByteItem.Value().([]byte)), val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := StringFromStackItem(intItem)
		require.Error(t, err)
	})
}
