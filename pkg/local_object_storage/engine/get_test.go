package engine

import (
	"io"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_GetBytes(t *testing.T) {
	e, _, _ := newEngine(t, t.TempDir())
	obj := generateObjectWithCID(cidtest.ID())
	addr := object.AddressOf(obj)

	objBin := obj.Marshal()

	err := e.Put(obj, nil)
	require.NoError(t, err)

	b, err := e.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)
}

func TestStorageEngine_GetStream(t *testing.T) {
	e, _, _ := newEngine(t, t.TempDir())
	obj := generateObjectWithCID(cidtest.ID())
	addr := object.AddressOf(obj)

	objBin := obj.Payload()

	err := e.Put(obj, nil)
	require.NoError(t, err)

	header, reader, err := e.GetStream(addr)
	require.NoError(t, err)
	require.Equal(t, obj.CutPayload(), header)

	require.NotNil(t, reader)
	b, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, objBin, b)
	require.NoError(t, reader.Close())
}
