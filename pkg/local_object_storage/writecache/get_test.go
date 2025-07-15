package writecache

import (
	"io"
	"testing"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestCache_GetBytes(t *testing.T) {
	const maxObjSize = 4 << 10
	c, _ := newCache(t)

	o := putObject(t, c, maxObjSize/2)
	objBin := o.obj.Marshal()

	b, err := c.GetBytes(o.addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)

	o = putObject(t, c, 2*maxObjSize)
	objBin = o.obj.Marshal()

	b, err = c.GetBytes(o.addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)
}

func TestCache_GetStream(t *testing.T) {
	const maxObjSize = 4 << 10
	c, _ := newCache(t)

	testStream := func(t *testing.T, size int) {
		o := putObject(t, c, size)
		objBin := o.obj.Payload()

		header, reader, err := c.GetStream(o.addr)
		require.NoError(t, err)
		require.Equal(t, o.obj.CutPayload(), header)

		require.NotNil(t, reader)
		b, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, objBin, b)
		require.NoError(t, reader.Close())
	}

	testStream(t, 0)
	testStream(t, maxObjSize/2)
	testStream(t, 2*maxObjSize)

	t.Run("not found", func(t *testing.T) {
		header, reader, err := c.GetStream(oidtest.Address())
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		require.Nil(t, header)
		require.Nil(t, reader)
	})
}
