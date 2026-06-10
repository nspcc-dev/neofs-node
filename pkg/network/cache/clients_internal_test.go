package cache

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/stretchr/testify/require"
)

func TestMultiEndpointError(t *testing.T) {
	firstErr := errors.New("test err")

	err := newMultiEndpointError("NODE_X", firstErr)
	require.EqualError(t, err, "all NODE_X endpoints failed, first error: test err")
	require.ErrorIs(t, err, firstErr)
	require.ErrorIs(t, err, clientcore.ErrAllConnectionsSkipped)
}
