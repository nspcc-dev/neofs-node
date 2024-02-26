package writecache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache_GetBytes(t *testing.T) {
	const maxObjSize = 4 << 10
	c, _, _ := newCache(t, maxObjSize)

	o := putObject(t, c, maxObjSize/2)
	objBin, err := o.obj.Marshal()
	require.NoError(t, err)

	b, err := c.GetBytes(o.addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)

	o = putObject(t, c, 2*maxObjSize)
	objBin, err = o.obj.Marshal()
	require.NoError(t, err)

	b, err = c.GetBytes(o.addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)
}
