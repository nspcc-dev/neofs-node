package implementations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateEpochParams(t *testing.T) {
	s := UpdateEpochParams{}

	e := uint64(100)
	s.SetNumber(e)

	require.Equal(t, e, s.Number())
}

func TestUpdateStateParams(t *testing.T) {
	s := UpdateStateParams{}

	st := NodeState(1)
	s.SetState(st)

	require.Equal(t, st, s.State())

	key := []byte{1, 2, 3}
	s.SetKey(key)

	require.Equal(t, key, s.Key())
}
