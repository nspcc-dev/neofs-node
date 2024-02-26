package engine

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_GetBytes(t *testing.T) {
	e, _, _ := newEngine(t, t.TempDir())
	obj := generateObjectWithCID(t, cidtest.ID())
	addr := object.AddressOf(obj)

	objBin, err := obj.Marshal()
	require.NoError(t, err)

	err = Put(e, obj)
	require.NoError(t, err)

	b, err := e.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)
}
