package basic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEqual(t *testing.T) {
	require.True(t,
		Equal(
			FromUint32(1),
			FromUint32(1),
		),
	)

	require.False(t,
		Equal(
			FromUint32(1),
			FromUint32(2),
		),
	)
}

func TestMarshal(t *testing.T) {
	a := FromUint32(1)
	a2 := new(ACL)

	require.NoError(t,
		a2.UnmarshalBinary(
			Marshal(a),
		),
	)

	require.True(t, Equal(a, *a2))
}
