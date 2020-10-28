package client

import (
	"math"
	"math/big"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/encoding/bigint"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/stretchr/testify/require"
)

var (
	stringParam = sc.Parameter{
		Type:  sc.StringType,
		Value: "Hello World",
	}

	intParam = sc.Parameter{
		Type:  sc.IntegerType,
		Value: int64(1),
	}

	byteWithIntParam = sc.Parameter{
		Type:  sc.ByteArrayType,
		Value: []byte{0x0a},
	}

	byteArrayParam = sc.Parameter{
		Type:  sc.ByteArrayType,
		Value: []byte("Hello World"),
	}

	emptyByteArrayParam = sc.Parameter{
		Type:  sc.ByteArrayType,
		Value: []byte{},
	}

	trueBoolParam = sc.Parameter{
		Type:  sc.BoolType,
		Value: true,
	}

	falseBoolParam = sc.Parameter{
		Type:  sc.BoolType,
		Value: false,
	}

	arrayParam = sc.Parameter{
		Type:  sc.ArrayType,
		Value: []sc.Parameter{intParam, byteArrayParam},
	}

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

func TestBoolFromStackParameter(t *testing.T) {
	t.Run("true assert", func(t *testing.T) {
		val, err := BoolFromStackParameter(trueBoolParam)
		require.NoError(t, err)
		require.True(t, val)

		val, err = BoolFromStackParameter(intParam)
		require.NoError(t, err)
		require.True(t, val)
	})

	t.Run("false assert", func(t *testing.T) {
		val, err := BoolFromStackParameter(falseBoolParam)
		require.NoError(t, err)
		require.False(t, val)

		val, err = BoolFromStackParameter(emptyByteArrayParam)
		require.NoError(t, err)
		require.False(t, val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := BoolFromStackParameter(stringParam)
		require.Error(t, err)
	})
}

func TestArrayFromStackParameter(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := ArrayFromStackParameter(arrayParam)
		require.NoError(t, err)
		require.Len(t, val, len(arrayParam.Value.([]sc.Parameter)))
	})
	t.Run("incorrect assert", func(t *testing.T) {
		_, err := ArrayFromStackParameter(byteArrayParam)
		require.Error(t, err)
	})
	t.Run("boolean false case", func(t *testing.T) {
		val, err := ArrayFromStackParameter(falseBoolParam)
		require.NoError(t, err)
		require.Nil(t, val)
	})
}

func TestBytesFromStackParameter(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := BytesFromStackParameter(byteArrayParam)
		require.NoError(t, err)
		require.Equal(t, byteArrayParam.Value.([]byte), val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := BytesFromStackParameter(stringParam)
		require.Error(t, err)
	})
}

func TestIntFromStackParameter(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := IntFromStackParameter(intParam)
		require.NoError(t, err)
		require.Equal(t, intParam.Value.(int64), val)

		val, err = IntFromStackParameter(byteWithIntParam)
		require.NoError(t, err)
		require.Equal(t, int64(0x0a), val)

		val, err = IntFromStackParameter(emptyByteArrayParam)
		require.NoError(t, err)
		require.Equal(t, int64(0), val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := IntFromStackParameter(byteArrayParam)
		require.Error(t, err)
	})
}

func TestStringFromStackParameter(t *testing.T) {
	t.Run("correct assert", func(t *testing.T) {
		val, err := StringFromStackParameter(stringParam)
		require.NoError(t, err)
		require.Equal(t, stringParam.Value.(string), val)

		val, err = StringFromStackParameter(byteArrayParam)
		require.NoError(t, err)
		require.Equal(t, string(byteArrayParam.Value.([]byte)), val)
	})

	t.Run("incorrect assert", func(t *testing.T) {
		_, err := StringFromStackParameter(intParam)
		require.Error(t, err)
	})
}

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
