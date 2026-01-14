package writecache

import (
	"io"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
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

func TestCache_GetRangeStream(t *testing.T) {
	c, _ := newCache(t)

	const pldLen = 1024
	pld := testutil.RandByteSlice(pldLen)

	obj := objecttest.Object()
	obj.SetPayload(pld)
	obj.SetPayloadSize(pldLen)

	addr := obj.Address()

	_, err := c.GetRangeStream(addr, 0, 0)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	_, err = c.GetRangeStream(addr, 1, pldLen-1)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	require.NoError(t, c.Put(addr, &obj, obj.Marshal()))

	_, err = c.GetRangeStream(addr, 1, 0)
	require.EqualError(t, err, "invalid range off=1,ln=0")

	for _, tc := range []struct{ off, ln uint64 }{
		{off: 0, ln: 0},
		{off: 0, ln: pldLen},
		{off: 1, ln: pldLen - 1},
		{off: pldLen - 1, ln: 1},
	} {
		stream, err := c.GetRangeStream(addr, tc.off, tc.ln)
		require.NoError(t, err, tc)

		b, err := io.ReadAll(stream)
		require.NoError(t, err)

		if tc.off == 0 && tc.ln == 0 {
			require.Equal(t, pld, b, tc)
		} else {
			require.Equal(t, pld[tc.off:][:tc.ln], b, tc)
		}

		require.NoError(t, stream.Close())
	}

	for _, tc := range []struct{ off, ln uint64 }{
		{off: 0, ln: pldLen + 1},
		{off: 1, ln: pldLen},
		{off: pldLen - 1, ln: 2},
	} {
		_, err := c.GetRangeStream(addr, tc.off, tc.ln)
		require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
	}

	require.NoError(t, c.Delete(addr))

	_, err = c.GetRangeStream(addr, 0, 0)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	_, err = c.GetRangeStream(addr, 1, pldLen-1)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
}
