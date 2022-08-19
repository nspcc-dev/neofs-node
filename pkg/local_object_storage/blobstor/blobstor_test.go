package blobstor

import (
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestCompression(t *testing.T) {
	dir, err := os.MkdirTemp("", "neofs*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	const (
		smallSizeLimit = 512
		objCount       = 4
	)

	newBlobStor := func(t *testing.T, compress bool) *BlobStor {
		bs := New(WithCompressObjects(compress),
			WithRootPath(dir),
			WithSmallSizeLimit(smallSizeLimit),
			WithBlobovniczaShallowWidth(1)) // default width is 16, slow init
		require.NoError(t, bs.Open(false))
		require.NoError(t, bs.Init())
		return bs
	}

	bigObj := make([]*objectSDK.Object, objCount)
	smallObj := make([]*objectSDK.Object, objCount)
	for i := 0; i < objCount; i++ {
		bigObj[i] = testObject(smallSizeLimit * 2)
		smallObj[i] = testObject(smallSizeLimit / 2)
	}

	testGet := func(t *testing.T, b *BlobStor, i int) {
		res1, err := b.Get(common.GetPrm{Address: object.AddressOf(smallObj[i])})
		require.NoError(t, err)
		require.Equal(t, smallObj[i], res1.Object)

		res2, err := b.Get(common.GetPrm{Address: object.AddressOf(bigObj[i])})
		require.NoError(t, err)
		require.Equal(t, bigObj[i], res2.Object)
	}

	testPut := func(t *testing.T, b *BlobStor, i int) {
		var prm common.PutPrm
		prm.Object = smallObj[i]
		_, err = b.Put(prm)
		require.NoError(t, err)

		prm = common.PutPrm{}
		prm.Object = bigObj[i]
		_, err = b.Put(prm)
		require.NoError(t, err)
	}

	// Put and Get uncompressed object
	blobStor := newBlobStor(t, false)
	testPut(t, blobStor, 0)
	testGet(t, blobStor, 0)
	require.NoError(t, blobStor.Close())

	blobStor = newBlobStor(t, true)
	testGet(t, blobStor, 0) // get uncompressed object with compress enabled
	testPut(t, blobStor, 1)
	testGet(t, blobStor, 1)
	require.NoError(t, blobStor.Close())

	blobStor = newBlobStor(t, false)
	testGet(t, blobStor, 0) // get old uncompressed object
	testGet(t, blobStor, 1) // get compressed object with compression disabled
	testPut(t, blobStor, 2)
	testGet(t, blobStor, 2)
	require.NoError(t, blobStor.Close())
}

func TestBlobstor_needsCompression(t *testing.T) {
	const smallSizeLimit = 512
	newBlobStor := func(t *testing.T, compress bool, ct ...string) *BlobStor {
		dir, err := os.MkdirTemp("", "neofs*")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })

		bs := New(WithCompressObjects(compress),
			WithRootPath(dir),
			WithSmallSizeLimit(smallSizeLimit),
			WithBlobovniczaShallowWidth(1),
			WithUncompressableContentTypes(ct))
		require.NoError(t, bs.Open(false))
		require.NoError(t, bs.Init())
		return bs
	}

	newObjectWithCt := func(contentType string) *objectSDK.Object {
		obj := testObject(smallSizeLimit + 1)
		if contentType != "" {
			var a objectSDK.Attribute
			a.SetKey(objectSDK.AttributeContentType)
			a.SetValue(contentType)
			obj.SetAttributes(a)
		}
		return obj
	}

	t.Run("content-types specified", func(t *testing.T) {
		b := newBlobStor(t, true, "audio/*", "*/x-mpeg", "*/mpeg", "application/x-midi")

		obj := newObjectWithCt("video/mpeg")
		require.False(t, b.NeedsCompression(obj))

		obj = newObjectWithCt("audio/aiff")
		require.False(t, b.NeedsCompression(obj))

		obj = newObjectWithCt("application/x-midi")
		require.False(t, b.NeedsCompression(obj))

		obj = newObjectWithCt("text/plain")
		require.True(t, b.NeedsCompression(obj))

		obj = newObjectWithCt("")
		require.True(t, b.NeedsCompression(obj))
	})
	t.Run("content-types omitted", func(t *testing.T) {
		b := newBlobStor(t, true)
		obj := newObjectWithCt("video/mpeg")
		require.True(t, b.NeedsCompression(obj))
	})
	t.Run("compress disabled", func(t *testing.T) {
		b := newBlobStor(t, false, "video/mpeg")

		obj := newObjectWithCt("video/mpeg")
		require.False(t, b.NeedsCompression(obj))

		obj = newObjectWithCt("text/plain")
		require.False(t, b.NeedsCompression(obj))
	})
}
