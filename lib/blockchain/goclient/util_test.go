package goclient

import (
	"testing"

	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
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
