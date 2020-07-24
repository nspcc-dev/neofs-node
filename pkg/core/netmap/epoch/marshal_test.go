package epoch

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEpochMarshal(t *testing.T) {
	e := FromUint64(1)
	e2 := new(Epoch)

	require.NoError(t,
		e2.UnmarshalBinary(
			Marshal(e),
		),
	)

	require.True(t, EQ(e, *e2))
}
