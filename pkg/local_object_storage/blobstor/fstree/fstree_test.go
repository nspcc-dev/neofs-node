package fstree

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"testing/iotest"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	iprotobuf "github.com/nspcc-dev/neofs-sdk-go/proto/protobuf"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestAddressToString(t *testing.T) {
	addr := oidtest.Address()
	s := stringifyAddress(addr)
	actual, err := addressFromString(s)
	require.NoError(t, err)
	require.Equal(t, addr, *actual)
}

func TestFSTree_GetRangeStream(t *testing.T) {
	testGetRangeStream(t, setupFSTree(t))
}

func TestFSTree_GetRangeStreamWithHeaderAndBounds(t *testing.T) {
	testGetRangeStreamFunc(t, setupFSTree(t), func(fst *FSTree, addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
		rng := common.NewPayloadRange(off, ln)
		if ln == 1 {
			rng = common.NewPayloadRangeBounds(off, off+ln-1)
		}
		hdr, payloadLen, stream, err := fst.GetRangeStream(addr, rng, true)
		if err == nil {
			require.NotNil(t, hdr)
			require.Equal(t, payloadLen, hdr.PayloadSize())
		} else {
			require.Nil(t, hdr)
		}
		return stream, err
	})
}

func TestFSTree_ReadPayloadRange(t *testing.T) {
	testReadPayloadRange(t, setupFSTree(t))
}

func testGetRangeStream(t *testing.T, fst *FSTree) {
	testGetRangeStreamFunc(t, fst, func(fst *FSTree, addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
		_, _, stream, err := fst.GetRangeStream(addr, common.NewPayloadRange(off, ln), false)
		return stream, err
	})
}

func testReadPayloadRange(t *testing.T, fst *FSTree) {
	testGetRangeStreamFunc(t, fst, func(fst *FSTree, addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
		return fst.ReadPayloadRange(addr, off, ln, make([]byte, 40<<10))
	})
}

func TestFSTree_PayloadRangeStreamsLimitBufferedPayload(t *testing.T) {
	const prefixLen = iobject.NonPayloadFieldsBufferLength
	const readBufLen = 2 * iobject.NonPayloadFieldsBufferLength

	readers := []struct {
		name string
		read func(*FSTree, oid.Address, uint64, uint64) (io.ReadCloser, error)
	}{
		{
			name: "ReadPayloadRange",
			read: func(fst *FSTree, addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
				return fst.ReadPayloadRange(addr, off, ln, make([]byte, readBufLen))
			},
		},
		{
			name: "GetRangeStream",
			read: func(fst *FSTree, addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
				_, _, stream, err := fst.GetRangeStream(addr, common.NewPayloadRange(off, ln), false)
				return stream, err
			},
		},
	}

	for _, tc := range []struct {
		name                     string
		payload                  []byte
		off, ln                  uint64
		padHeaderToPayloadOffset bool
	}{
		{
			name:                     "empty payload prefix stream",
			payload:                  []byte("payload"),
			ln:                       3,
			padHeaderToPayloadOffset: true,
		},
		{
			name:    "range inside payload prefix stream",
			payload: testutil.RandByteSlice(2 * prefixLen),
			off:     222,
			ln:      378,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fst := setupFSTree(t)
			objWire := objectWireForPayloadRangeStreamTest(t, tc.payload, tc.padHeaderToPayloadOffset)

			addr := oidtest.Address()
			require.NoError(t, fst.Put(addr, objWire))

			for _, reader := range readers {
				t.Run(reader.name, func(t *testing.T) {
					stream, err := reader.read(fst, addr, tc.off, tc.ln)
					require.NoError(t, err)
					defer stream.Close()

					actual, err := io.ReadAll(stream)
					require.NoError(t, err)
					require.Equal(t, tc.payload[tc.off:tc.off+tc.ln], actual)
				})
			}
		})
	}
}

func objectWireForPayloadRangeStreamTest(t *testing.T, payload []byte, padHeaderToPayloadOffset bool) []byte {
	t.Helper()

	baseHeader := protowire.AppendTag(nil, protoobject.FieldHeaderPayloadLength, protowire.VarintType)
	baseHeader = protowire.AppendVarint(baseHeader, uint64(len(payload)))

	if padHeaderToPayloadOffset {
		// keep payload bytes out of the initially buffered prefix while preserving valid protobuf wire
		const prefixLen = iobject.NonPayloadFieldsBufferLength
		payloadPrefixLen := protowire.SizeTag(protoobject.FieldObjectPayload) + protowire.SizeVarint(uint64(len(payload)))
		headerPaddingFieldNum := protowire.Number(protoobject.FieldHeaderPayloadLength + 1)
		headerPaddingLen := prefixLen
		for {
			headerPaddingFieldLen := protowire.SizeTag(headerPaddingFieldNum) + protowire.SizeVarint(uint64(headerPaddingLen)) + headerPaddingLen
			headerLen := len(baseHeader) + headerPaddingFieldLen
			n := prefixLen - protowire.SizeTag(protoobject.FieldObjectHeader) - protowire.SizeVarint(uint64(headerLen)) - len(baseHeader) - protowire.SizeTag(headerPaddingFieldNum) - protowire.SizeVarint(uint64(headerPaddingLen)) - payloadPrefixLen
			require.GreaterOrEqual(t, n, 0)
			if n == headerPaddingLen {
				break
			}
			headerPaddingLen = n
		}

		baseHeader = protowire.AppendTag(baseHeader, headerPaddingFieldNum, protowire.BytesType)
		baseHeader = protowire.AppendBytes(baseHeader, make([]byte, headerPaddingLen))
	}

	objWire := protowire.AppendTag(nil, protoobject.FieldObjectHeader, protowire.BytesType)
	objWire = protowire.AppendBytes(objWire, baseHeader)
	objWire = protowire.AppendTag(objWire, protoobject.FieldObjectPayload, protowire.BytesType)
	objWire = protowire.AppendVarint(objWire, uint64(len(payload)))
	if padHeaderToPayloadOffset {
		require.Len(t, objWire, iobject.NonPayloadFieldsBufferLength)
	}

	return append(objWire, payload...)
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
	require.ErrorIs(t, err, apistatus.ErrObjectOutOfRange)

	for _, tc := range []struct{ off, ln uint64 }{
		{off: 0, ln: 0},
		{off: 0, ln: pldLen / 2},
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
