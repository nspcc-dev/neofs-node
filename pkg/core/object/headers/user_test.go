package headers

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/stretchr/testify/require"
)

func TestUserHeader_Key(t *testing.T) {
	h := new(UserHeader)

	key := "random key"
	h.SetKey(key)

	require.Equal(t, key, h.Key())
}

func TestUserHeader_Value(t *testing.T) {
	h := new(UserHeader)

	val := "random value"
	h.SetValue(val)

	require.Equal(t, val, h.Value())
}

func TestNewUserHeader(t *testing.T) {
	key := "user key"
	val := "user val"

	h := NewUserHeader(key, val)

	require.True(t,
		object.TypesEQ(
			TypeUser,
			h.Type(),
		),
	)

	uh := h.Value().(*UserHeader)

	require.Equal(t, key, uh.Key())
	require.Equal(t, val, uh.Value())
}
