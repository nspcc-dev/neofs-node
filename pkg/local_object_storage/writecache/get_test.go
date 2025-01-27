package writecache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache_GetBytes(t *testing.T) {
	const maxObjSize = 4 << 10
	c, _, _ := newCache(t)

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
