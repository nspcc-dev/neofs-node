package object

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtendedHeader_Type(t *testing.T) {
	h := new(ExtendedHeader)

	ht := TypeFromUint32(3)
	h.SetType(ht)

	require.True(t, TypesEQ(ht, h.Type()))
}

func TestExtendedHeader_Value(t *testing.T) {
	h := new(ExtendedHeader)

	val := 100
	h.SetValue(val)

	require.Equal(t, val, h.Value())
}
