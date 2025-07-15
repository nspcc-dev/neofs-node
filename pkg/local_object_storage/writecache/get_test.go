package writecache

import (
	"io"
	"testing"

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

	testStream(t, maxObjSize/2)
	testStream(t, 2*maxObjSize)
}
