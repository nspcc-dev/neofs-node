package object_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"math"
	"slices"
	"testing"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	neofscryptotest "github.com/nspcc-dev/neofs-sdk-go/crypto/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/nspcc-dev/tzhash/tz"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestFields(t *testing.T) {
	require.EqualValues(t, 1, iobject.FieldHeaderVersion)
	require.EqualValues(t, 2, iobject.FieldHeaderContainerID)
	require.EqualValues(t, 3, iobject.FieldHeaderOwnerID)
	require.EqualValues(t, 4, iobject.FieldHeaderCreationEpoch)
	require.EqualValues(t, 5, iobject.FieldHeaderPayloadLength)
	require.EqualValues(t, 6, iobject.FieldHeaderPayloadHash)
	require.EqualValues(t, 7, iobject.FieldHeaderType)
	require.EqualValues(t, 8, iobject.FieldHeaderHomoHash)
	require.EqualValues(t, 9, iobject.FieldHeaderSessionToken)
	require.EqualValues(t, 10, iobject.FieldHeaderAttributes)
	require.EqualValues(t, 11, iobject.FieldHeaderSplit)
	require.EqualValues(t, 12, iobject.FieldHeaderSessionTokenV2)

	require.EqualValues(t, 1, iobject.FieldHeaderSplitParent)
	require.EqualValues(t, 2, iobject.FieldHeaderSplitPrevious)
	require.EqualValues(t, 3, iobject.FieldHeaderSplitParentSignature)
	require.EqualValues(t, 4, iobject.FieldHeaderSplitParentHeader)
	require.EqualValues(t, 5, iobject.FieldHeaderSplitChildren)
	require.EqualValues(t, 6, iobject.FieldHeaderSplitSplitID)
	require.EqualValues(t, 7, iobject.FieldHeaderSplitFirst)
}

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

func TestGetNonPayloadFieldBounds(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		_, _, _, err := iobject.GetNonPayloadFieldBounds([]byte{})
		require.EqualError(t, err, "empty data")
	})

	id := oidtest.ID()
	sig := neofscryptotest.Signature()

	obj := objecttest.Object()
	obj.SetID(id)
	obj.SetSignature(&sig)

	buf := obj.Marshal()

	idf, sigf, hdrf, err := iobject.GetNonPayloadFieldBounds(buf)
	require.NoError(t, err)

	assertFound := func(t *testing.T, f iprotobuf.FieldBounds, tag byte, exp []byte) {
		require.False(t, f.IsMissing())
		require.EqualValues(t, tag, buf[f.From])
		ln, n, err := iprotobuf.ParseLENField(buf[f.From+1:], 42, protowire.BytesType)
		require.NoError(t, err)
		require.EqualValues(t, 1+n, f.ValueFrom-f.From)
		require.EqualValues(t, ln, f.To-f.ValueFrom)
		require.True(t, bytes.Equal(exp, buf[f.ValueFrom:f.To]))
	}

	assertFound(t, idf, iprotobuf.TagBytes1, id.Marshal())
	assertFound(t, sigf, iprotobuf.TagBytes2, sig.Marshal())

	hdr := obj.ProtoMessage().Header
	hdrBuf := make([]byte, hdr.MarshaledSize())
	hdr.MarshalStable(hdrBuf)
	assertFound(t, hdrf, iprotobuf.TagBytes3, hdrBuf)
}

func BenchmarkGetNonPayloadFieldBounds(b *testing.B) {
	id := oidtest.ID()
	const sigLen = 100
	const hdrLen = 16 << 10

	buf := slices.Concat(
		[]byte{iprotobuf.TagBytes1, oid.Size + 2, iprotobuf.TagBytes1, oid.Size}, id[:],
		[]byte{iprotobuf.TagBytes2, sigLen}, testutil.RandByteSlice(sigLen),
		[]byte{iprotobuf.TagBytes3, 128, 128, 1}, testutil.RandByteSlice(hdrLen),
	)

	idf, sigf, hdrf, err := iobject.GetNonPayloadFieldBounds(buf)
	require.NoError(b, err)
	require.EqualValues(b, 0, idf.From)
	require.EqualValues(b, idf.From+2, idf.ValueFrom)
	require.EqualValues(b, idf.ValueFrom+oid.Size, idf.To)
	require.EqualValues(b, idf.To, sigf.From)
	require.EqualValues(b, sigf.From+2, sigf.ValueFrom)
	require.EqualValues(b, sigf.ValueFrom+sigLen, sigf.To)
	require.EqualValues(b, sigf.To, hdrf.From)
	require.EqualValues(b, hdrf.From+4, hdrf.ValueFrom)
	require.EqualValues(b, hdrf.ValueFrom+hdrLen, hdrf.To)
	require.EqualValues(b, len(buf), hdrf.To)

	b.ReportAllocs()
	for b.Loop() {
		_, _, _, err = iobject.GetNonPayloadFieldBounds(buf)
		require.NoError(b, err)
	}
}

func TestGetParentNonPayloadFieldBounds(t *testing.T) {
	t.Run("empty data", func(t *testing.T) {
		_, _, _, err := iobject.GetParentNonPayloadFieldBounds([]byte{})
		require.EqualError(t, err, "empty data")
	})

	parID := oidtest.ID()
	parSig := neofscryptotest.Signature()

	par := objecttest.Object()
	par.SetID(parID)
	par.SetSignature(&parSig)
	par.ResetRelations()

	obj := objecttest.Object()
	obj.SetParent(&par)

	buf := obj.Marshal()

	idf, sigf, hdrf, err := iobject.GetParentNonPayloadFieldBounds(buf)
	require.NoError(t, err)

	assertFound := func(t *testing.T, f iprotobuf.FieldBounds, tag byte, exp []byte) {
		require.False(t, f.IsMissing())
		require.EqualValues(t, tag, buf[f.From])
		ln, n, err := iprotobuf.ParseLENField(buf[f.From+1:], 42, protowire.BytesType)
		require.NoError(t, err)
		require.EqualValues(t, 1+n, f.ValueFrom-f.From)
		require.EqualValues(t, ln, f.To-f.ValueFrom)
		require.True(t, bytes.Equal(exp, buf[f.ValueFrom:f.To]))
	}

	assertFound(t, idf, iprotobuf.TagBytes1, parID.Marshal())
	assertFound(t, sigf, iprotobuf.TagBytes3, parSig.Marshal())

	parHdr := par.ProtoMessage().Header
	parHdrBuf := make([]byte, parHdr.MarshaledSize())
	parHdr.MarshalStable(parHdrBuf)
	assertFound(t, hdrf, iprotobuf.TagBytes4, parHdrBuf)
}

func BenchmarkGetParentNonPayloadFieldBounds(b *testing.B) {
	parID := oidtest.ID()

	sig := neofscrypto.NewSignatureFromRawKey(neofscrypto.ECDSA_DETERMINISTIC_SHA256, testutil.RandByteSlice(33), testutil.RandByteSlice(64))
	ver := version.New(123, 456)
	pldHash := checksum.New(checksum.SHA256, testutil.RandByteSlice(sha256.Size))
	pldHomoHash := checksum.New(checksum.TillichZemor, testutil.RandByteSlice(tz.Size))

	fillHeader := func(obj *object.Object) {
		obj.SetID(oidtest.ID())
		obj.SetSignature(&sig)
		obj.SetVersion(&ver)
		obj.SetContainerID(cidtest.ID())
		obj.SetCreationEpoch(math.MaxUint64)
		obj.SetPayloadSize(math.MaxUint64)
		obj.SetPayloadChecksum(pldHash)
		obj.SetType(math.MaxInt32)
		obj.SetPayloadHomomorphicHash(pldHomoHash)
		obj.SetAttributes(
			object.NewAttribute("key1", "val1"),
			object.NewAttribute("key2", "val2"),
			object.NewAttribute("key3", "val3"),
		)
	}

	var par object.Object
	fillHeader(&par)
	par.SetID(parID)

	var obj object.Object
	fillHeader(&obj)
	obj.SetPreviousID(oidtest.ID())
	obj.SetParent(&par)

	buf := obj.Marshal()

	idf, sigf, hdrf, err := iobject.GetParentNonPayloadFieldBounds(buf)
	require.NoError(b, err)
	require.Positive(b, idf.From)
	require.EqualValues(b, idf.From+2, idf.ValueFrom)
	require.EqualValues(b, idf.ValueFrom+2+oid.Size, idf.To)
	require.EqualValues(b, idf.To+2+2+oid.Size, sigf.From)
	require.EqualValues(b, sigf.From+2, sigf.ValueFrom)
	require.EqualValues(b, sigf.ValueFrom+len(sig.Marshal()), sigf.To)
	require.EqualValues(b, sigf.To, hdrf.From)
	require.EqualValues(b, hdrf.From+3, hdrf.ValueFrom)
	require.EqualValues(b, hdrf.ValueFrom+par.HeaderLen(), hdrf.To)
	require.EqualValues(b, len(buf), hdrf.To)

	b.ReportAllocs()
	for b.Loop() {
		_, _, _, err = iobject.GetNonPayloadFieldBounds(buf)
		require.NoError(b, err)
	}
}
