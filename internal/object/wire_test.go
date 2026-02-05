package object_test

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestWriteWithoutPayload(t *testing.T) {
	t.Run("write empty object", func(t *testing.T) {
		var buf bytes.Buffer
		require.NoError(t, iobject.WriteWithoutPayload(&buf, object.Object{}))
		require.Empty(t, buf.Bytes())
	})

	obj := new(object.Object)
	payload := []byte{1, 2, 3, 4, 5}
	obj.SetPayloadSize(uint64(len(payload)))

	var buf bytes.Buffer
	require.NoError(t, iobject.WriteWithoutPayload(&buf, *obj))

	result := buf.Bytes()
	require.NotEmpty(t, result)

	// 34 is the tag for field 4 (payload) with wire type 2
	// and 5 is the length of the payload
	expected := append(obj.CutPayload().Marshal(), 34, 5)
	require.Equal(t, expected, result)
}

func TestExtractHeaderAndPayload(t *testing.T) {
	t.Run("extract from empty data", func(t *testing.T) {
		_, _, err := iobject.ExtractHeaderAndPayload(nil)
		require.ErrorContains(t, err, "empty data")
	})

	t.Run("wrong wire type", func(t *testing.T) {
		// tag for field 1 with wire type 0 (varint), but expecting wire type 2 (bytes)
		b := []byte{8, 0}
		_, _, err := iobject.ExtractHeaderAndPayload(b)
		require.ErrorContains(t, err, "unexpected wire type")
	})

	t.Run("unknown field", func(t *testing.T) {
		// tag for field 5 with wire type 2, but field 5 is not defined in the object
		b := []byte{42, 0}
		_, _, err := iobject.ExtractHeaderAndPayload(b)
		require.ErrorContains(t, err, "unknown field")
	})

	t.Run("truncated payload varint", func(t *testing.T) {
		// tag for field 4 with wire type 2 and a varint length 0x80, that is truncated
		b := []byte{34, 0x80}
		_, _, err := iobject.ExtractHeaderAndPayload(b)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.ErrorContains(t, err, "invalid varint at offset")
	})

	t.Run("truncated bytes field", func(t *testing.T) {
		// tag for field 3 with wire type 2 and a varint length 10, but no bytes provided
		b := []byte{26, 10}
		_, _, err := iobject.ExtractHeaderAndPayload(b)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.ErrorContains(t, err, "invalid bytes field at offset")
	})
}

func TestReadHeaderPrefix(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		r := bytes.NewReader(nil)
		_, _, err := iobject.ReadHeaderPrefix(r)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	const size = 16 << 10 // 16 KB
	payload := make([]byte, size)
	_, _ = rand.Read(payload)

	obj := objecttest.Object()
	obj.SetPayload(payload)
	obj.SetPayloadSize(uint64(size))

	hdr, payloadPrefix, err := iobject.ReadHeaderPrefix(bytes.NewReader(obj.Marshal()))
	require.NoError(t, err)
	require.NotNil(t, hdr)
	require.Equal(t, obj.CutPayload(), hdr)

	headerLen := len(hdr.CutPayload().Marshal()) + 4 // 4 bytes for the payload length varint
	expectedSize := object.MaxHeaderLen - headerLen
	require.Equal(t, expectedSize, len(payloadPrefix))
	require.Equal(t, payload[:expectedSize], payloadPrefix)
}

func TestSeekHeaderFields(t *testing.T) {
	const pubkeyLen = 33
	id := oidtest.ID()
	sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.N3, testutil.RandByteSlice(pubkeyLen), testutil.RandByteSlice(keys.SignatureLen))
	payload := []byte("Hello, world!")

	obj := objecttest.Object()
	obj.SetID(id)
	obj.SetSignature(&sig)
	obj.SetPayload(payload)

	hdrLen := obj.HeaderLen()
	hdrLenVarint := binary.PutVarint(make([]byte, binary.MaxVarintLen64), int64(hdrLen))

	encodeBuffer := func(obj []byte) []byte {
		b := testutil.RandByteSlice(object.MaxHeaderLen * 2)
		copy(b, obj)
		return b
	}

	encodeObject := func(obj object.Object) []byte {
		return encodeBuffer(obj.Marshal())
	}

	assertID := func(t *testing.T, data []byte, f iprotobuf.FieldBounds, off int) {
		require.EqualValues(t, off, f.From)
		require.EqualValues(t, f.From+(1+1), f.ValueFrom)
		const idFldLen = 1 + 1 + 32
		require.EqualValues(t, f.ValueFrom+idFldLen, f.To)
		require.Equal(t, []byte{10, idFldLen}, data[f.From:f.ValueFrom])
		var gotID oid.ID
		require.NoError(t, gotID.Unmarshal(data[f.ValueFrom:f.To]))
		require.Equal(t, obj.GetID(), gotID)
	}
	assertSignature := func(t *testing.T, data []byte, f iprotobuf.FieldBounds, off int) {
		require.EqualValues(t, off, f.From)
		require.EqualValues(t, f.From+(1+1), f.ValueFrom)
		const sigFldLen = (1 + 1 + pubkeyLen) + (1 + 1 + keys.SignatureLen) + (1 + 1)
		require.EqualValues(t, f.ValueFrom+sigFldLen, f.To)
		require.Equal(t, []byte{18, sigFldLen}, data[f.From:f.ValueFrom])
		var gotSig neofscrypto.Signature
		require.NoError(t, gotSig.Unmarshal(data[f.ValueFrom:f.To]))
		require.Equal(t, sig, gotSig)
	}
	assertHeader := func(t *testing.T, data []byte, f iprotobuf.FieldBounds, off int) {
		require.EqualValues(t, off, f.From)
		require.EqualValues(t, f.From+(1+hdrLenVarint), f.ValueFrom)
		require.EqualValues(t, f.ValueFrom+hdrLen, f.To)
		require.Equal(t, binary.AppendUvarint([]byte{26}, uint64(hdrLen)), data[f.From:f.ValueFrom])
		var gotHdr protoobject.Header
		require.NoError(t, proto.Unmarshal(data[f.ValueFrom:f.To], &gotHdr))
		require.True(t, proto.Equal(obj.ProtoMessage().Header, &gotHdr))
	}

	t.Run("empty", func(t *testing.T) {
		_, _, _, err := iobject.SeekHeaderFields(nil)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.EqualError(t, err, "parse field tag: "+io.ErrUnexpectedEOF.Error())

		_, _, _, err = iobject.SeekHeaderFields([]byte{})
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.EqualError(t, err, "parse field tag: "+io.ErrUnexpectedEOF.Error())
	})
	t.Run("payload tag", func(t *testing.T) {
		idf, sigf, hdrf, err := iobject.SeekHeaderFields([]byte{34})
		require.NoError(t, err)

		require.True(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())
	})
	t.Run("payload tag and len", func(t *testing.T) {
		idf, sigf, hdrf, err := iobject.SeekHeaderFields([]byte{34, 13})
		require.NoError(t, err)

		require.True(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())
	})
	t.Run("payload", func(t *testing.T) {
		var obj object.Object
		obj.SetPayload(payload)

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.True(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())
	})
	t.Run("id,signature,payload", func(t *testing.T) {
		var obj object.Object
		obj.SetID(id)
		obj.SetSignature(&sig)
		obj.SetPayload(payload)

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.False(t, idf.IsMissing())
		require.False(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())

		assertID(t, data, idf, 0)
		assertSignature(t, data, sigf, idf.To)
	})
	t.Run("id,header,payload", func(t *testing.T) {
		obj := obj
		obj.SetSignature(nil)

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.False(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.False(t, hdrf.IsMissing())

		assertID(t, data, idf, 0)
		assertHeader(t, data, hdrf, idf.To)
	})
	t.Run("id,signature", func(t *testing.T) {
		var obj object.Object
		obj.SetID(id)
		obj.SetSignature(&sig)

		data := obj.Marshal()

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.False(t, idf.IsMissing())
		require.False(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())

		assertID(t, data, idf, 0)
		assertSignature(t, data, sigf, idf.To)
	})
	t.Run("id,header", func(t *testing.T) {
		obj := obj
		obj.SetSignature(nil)
		obj.SetPayload(nil)

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.False(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.False(t, hdrf.IsMissing())

		assertID(t, data, idf, 0)
		assertHeader(t, data, hdrf, idf.To)
	})
	t.Run("id,payload", func(t *testing.T) {
		var obj object.Object
		obj.SetID(id)
		obj.SetPayload(payload)

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.False(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())

		assertID(t, data, idf, 0)
	})
	t.Run("id", func(t *testing.T) {
		var obj object.Object
		obj.SetID(id)

		data := obj.Marshal()

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.False(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())

		assertID(t, data, idf, 0)
	})
	t.Run("signature,header,payload", func(t *testing.T) {
		obj := obj
		obj.SetID(oid.ID{})

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.True(t, idf.IsMissing())
		require.False(t, sigf.IsMissing())
		require.False(t, hdrf.IsMissing())

		require.True(t, idf.IsMissing())
		assertSignature(t, data, sigf, 0)
		assertHeader(t, data, hdrf, sigf.To)
	})
	t.Run("signature,payload", func(t *testing.T) {
		var obj object.Object
		obj.SetSignature(&sig)
		obj.SetPayload(payload)

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.True(t, idf.IsMissing())
		require.False(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())

		assertSignature(t, data, sigf, 0)
	})
	t.Run("signature", func(t *testing.T) {
		var obj object.Object
		obj.SetSignature(&sig)

		data := obj.Marshal()

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.True(t, idf.IsMissing())
		require.False(t, sigf.IsMissing())
		require.True(t, hdrf.IsMissing())

		assertSignature(t, data, sigf, 0)
	})
	t.Run("header,payload", func(t *testing.T) {
		obj := obj
		obj.SetID(oid.ID{})
		obj.SetSignature(nil)

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.True(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.False(t, hdrf.IsMissing())

		assertHeader(t, data, hdrf, 0)
	})
	t.Run("header", func(t *testing.T) {
		obj := obj
		obj.SetID(oid.ID{})
		obj.SetSignature(nil)
		obj.SetPayload(nil)

		data := encodeObject(obj)

		idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
		require.NoError(t, err)

		require.True(t, idf.IsMissing())
		require.True(t, sigf.IsMissing())
		require.False(t, hdrf.IsMissing())

		assertHeader(t, data, hdrf, 0)
	})

	data := encodeObject(obj)

	idf, sigf, hdrf, err := iobject.SeekHeaderFields(data)
	require.NoError(t, err)

	require.False(t, idf.IsMissing())
	require.False(t, sigf.IsMissing())
	require.False(t, hdrf.IsMissing())

	assertID(t, data, idf, 0)
	assertSignature(t, data, sigf, idf.To)
	assertHeader(t, data, hdrf, sigf.To)
}
