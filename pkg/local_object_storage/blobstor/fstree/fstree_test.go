package fstree

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestAddressToString(t *testing.T) {
	addr := oidtest.Address()
	s := stringifyAddress(addr)
	actual, err := addressFromString(s)
	require.NoError(t, err)
	require.Equal(t, addr, *actual)
}

func TestFSTree_GetRangeStream(t *testing.T) {
	t.Run("compressed", func(t *testing.T) {
		comp := compression.Config{Enabled: true}
		require.NoError(t, comp.Init())

		fst := setupFSTree(t)
		fst.SetCompressor(&comp)

		testGetRangeStream(t, fst)
	})

	testGetRangeStream(t, setupFSTree(t))
}

func testGetRangeStream(t *testing.T, fst *FSTree) {
	const pldLen = 1024
	pld := testutil.RandByteSlice(pldLen)

	obj := objecttest.Object()
	obj.SetPayload(pld)
	obj.SetPayloadSize(pldLen)

	addr := obj.Address()

	_, err := fst.GetRangeStream(addr, 0, 0)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	_, err = fst.GetRangeStream(addr, 1, pldLen-1)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	require.NoError(t, fst.Put(addr, obj.Marshal()))

	_, err = fst.GetRangeStream(addr, 1, 0)
	require.EqualError(t, err, "invalid range off=1,ln=0")

	for _, tc := range []struct{ off, ln uint64 }{
		{off: 0, ln: 0},
		{off: 0, ln: pldLen},
		{off: 1, ln: pldLen - 1},
		{off: pldLen - 1, ln: 1},
	} {
		stream, err := fst.GetRangeStream(addr, tc.off, tc.ln)
		require.NoError(t, err, tc)

		if tc.off == 0 && tc.ln == 0 {
			require.NoError(t, iotest.TestReader(stream, pld))
		} else {
			require.NoError(t, iotest.TestReader(stream, pld[tc.off:][:tc.ln]))
		}

		require.NoError(t, stream.Close())
	}

	for _, tc := range []struct{ off, ln uint64 }{
		{off: 0, ln: pldLen + 1},
		{off: 1, ln: pldLen},
		{off: pldLen - 1, ln: 2},
	} {
		_, err := fst.GetRangeStream(addr, tc.off, tc.ln)
		require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
	}

	require.NoError(t, fst.Delete(addr))

	_, err = fst.GetRangeStream(addr, 0, 0)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	_, err = fst.GetRangeStream(addr, 1, pldLen-1)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
}

func assertOpenStreamOK(t *testing.T, fst *FSTree, addr oid.Address, data []byte) {
	_assertOpenStreamOK(t, fst, addr, data, -1, -1)
}

func assertOpenStreamOKWithBufferLen(t *testing.T, fst *FSTree, addr oid.Address, data []byte, bufLen int) {
	_assertOpenStreamOK(t, fst, addr, data, bufLen, -1)
}

func assertOpenStreamOKWithPrefixLen(t *testing.T, fst *FSTree, addr oid.Address, data []byte, prefixLen int) {
	_assertOpenStreamOK(t, fst, addr, data, -1, prefixLen)
}

func _assertOpenStreamOK(t *testing.T, fst *FSTree, addr oid.Address, data []byte, bufLen int, prefixLen int) {
	buf, n, reader, err := _openStream(fst, addr, bufLen)
	require.NoError(t, err)
	require.NotNil(t, reader)
	require.GreaterOrEqual(t, len(buf), n)
	if prefixLen >= 0 {
		require.EqualValues(t, prefixLen, n)
	}

	require.NoError(t, iotest.TestReader(io.MultiReader(bytes.NewReader(buf[:n]), reader), data))
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}

func openStream(fst *FSTree, addr oid.Address) ([]byte, int, io.ReadCloser, error) {
	return _openStream(fst, addr, -1)
}

func _openStream(fst *FSTree, addr oid.Address, bufLen int) ([]byte, int, io.ReadCloser, error) {
	if bufLen < 0 {
		bufLen = 42
	}

	var buf []byte

	n, stream, err := fst.OpenStream(addr, func() []byte {
		if buf == nil {
			buf = make([]byte, bufLen)
		}
		return buf
	})

	return buf, n, stream, err
}
