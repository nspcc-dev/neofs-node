package ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNode(t *testing.T) {
	s := Node{}

	key := []byte{1, 2, 3}
	s.SetKey(key)

	require.Equal(t, key, s.Key())
}
