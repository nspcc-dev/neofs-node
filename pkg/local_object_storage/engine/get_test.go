package engine

import (
	"io"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_GetBytes(t *testing.T) {
	e, _, _ := newEngine(t, t.TempDir())
	obj := generateObjectWithCID(cidtest.ID())
	addr := objectcore.AddressOf(obj)

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
	addr := objectcore.AddressOf(obj)

	err := e.Put(obj, nil)
	require.NoError(t, err)

	header, reader, err := e.GetStream(addr)
	assertGetStreamOK(t, header, reader, err, *obj)
}

func assertGetStreamOK(t *testing.T, header *object.Object, reader io.ReadCloser, err error, exp object.Object) {
	require.NoError(t, err)
	require.Equal(t, exp.CutPayload(), header)

	require.NotNil(t, reader)
	b, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, exp.Payload(), b)
	require.NoError(t, reader.Close())
}
