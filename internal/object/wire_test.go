package object_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	iobject "github.com/nspcc-dev/neofs-node/internal/object"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
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
