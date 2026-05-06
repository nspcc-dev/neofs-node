package fstree

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"testing/iotest"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

func TestFSTree_ReadPayloadRange(t *testing.T) {
	t.Run("compressed", func(t *testing.T) {
		comp := compression.Config{Enabled: true}
		require.NoError(t, comp.Init())

		fst := setupFSTree(t)
		fst.SetCompressor(&comp)

		testReadPayloadRange(t, fst)
	})

	testReadPayloadRange(t, setupFSTree(t))
}

func testGetRangeStream(t *testing.T, fst *FSTree) {
	testGetRangeStreamFunc(t, fst, func(fst *FSTree, addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
		_, stream, err := fst.GetRangeStream(addr, off, ln)
		return stream, err
	})
}

func testReadPayloadRange(t *testing.T, fst *FSTree) {
	testGetRangeStreamFunc(t, fst, func(fst *FSTree, addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
		return fst.ReadPayloadRange(addr, off, ln, make([]byte, 40<<10))
	})
}

func testGetRangeStreamFunc(t *testing.T, fst *FSTree, fn func(fst *FSTree, addr oid.Address, off, ln uint64) (io.ReadCloser, error)) {
	const pldLen = 1024
	pld := testutil.RandByteSlice(pldLen)

	obj := objecttest.Object()
	obj.SetPayload(pld)
	obj.SetPayloadSize(pldLen)

	addr := obj.Address()

	_, err := fn(fst, addr, 0, 0)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	_, err = fn(fst, addr, 1, pldLen-1)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	require.NoError(t, fst.Put(addr, obj.Marshal()))

	_, err = fn(fst, addr, 1, 0)
	require.EqualError(t, err, "invalid range off=1,ln=0")

	for _, tc := range []struct{ off, ln uint64 }{
		{off: 0, ln: 0},
		{off: 0, ln: pldLen},
		{off: 1, ln: pldLen - 1},
		{off: pldLen - 1, ln: 1},
	} {
		stream, err := fn(fst, addr, tc.off, tc.ln)
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
		_, err := fn(fst, addr, tc.off, tc.ln)
		require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)
	}

	require.NoError(t, fst.Delete(addr))

	_, err = fn(fst, addr, 0, 0)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	_, err = fn(fst, addr, 1, pldLen-1)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
}

func TestFSTree_PutBatch(t *testing.T) {
	t.Run("compressed", func(t *testing.T) {
		fst := setupFSTree(t)

		compCfg := &compression.Config{Enabled: true}
		require.NoError(t, compCfg.Init())
		fst.SetCompressor(compCfg)

		testPutBatch(t, fst)
	})

	testPutBatch(t, setupFSTree(t))
}

func testPutBatch(t *testing.T, fst *FSTree) {
	const pldLen = object.MaxHeaderLen

	objs := make([]object.Object, 3)
	batch := make(map[oid.Address][]byte)
	for i := range objs {
		objs[i] = objecttest.Object()
		objs[i].SetPayloadSize(pldLen)
		objs[i].SetPayload(testutil.RandByteSlice(pldLen))

		batch[objs[i].Address()] = objs[i].Marshal()
	}

	require.NoError(t, fst.PutBatch(batch))

	for i := range objs {
		hdr, stream, err := fst.GetStream(objs[i].Address())
		require.NoError(t, err)
		t.Cleanup(func() { stream.Close() })

		require.EqualValues(t, objs[i].CutPayload(), hdr)

		// note: iotest.TestReader does not fit due to overridden io.Seeker interface
		b, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.Len(t, b, pldLen)
		require.True(t, bytes.Equal(objs[i].Payload(), b))
	}
}

func assertReadObjectOK(t *testing.T, fst *FSTree, addr oid.Address, obj object.Object) {
	buf := make([]byte, 2*iobject.NonPayloadFieldsBufferLength)

	n, reader, err := fst.ReadObject(addr, buf)
	require.NoError(t, err)

	_, tail, ok := bytes.Cut(buf[:n], obj.CutPayload().Marshal())
	require.True(t, ok)

	prefix := make([]byte, 1+binary.MaxVarintLen64)
	prefix[0] = iprotobuf.TagBytes4 // payload field tag
	prefix = prefix[:1+binary.PutUvarint(prefix[1:], uint64(len(obj.Payload())))]

	if len(tail) < len(prefix) {
		require.True(t, bytes.HasPrefix(prefix, tail))
		return
	}

	tail, ok = bytes.CutPrefix(tail, prefix)
	require.True(t, ok)
	require.True(t, bytes.HasPrefix(obj.Payload(), tail))

	require.NoError(t, iotest.TestReader(io.MultiReader(bytes.NewReader(buf[:n]), reader), obj.Marshal()))
	require.NoError(t, reader.Close())
}
