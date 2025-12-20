package shard_test

import (
	"os"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
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

	newShardCompression := func(t *testing.T, compress bool) *shard.Shard {
		return newCustomShard(t, dir, false, nil,
			shard.WithCompressObjects(compress),
			shard.WithBlobstor(fstree.New(fstree.WithPath(dir))),
			shard.WithMode(mode.Degraded),
		)
	}

	bigObj := make([]*object.Object, objCount)
	smallObj := make([]*object.Object, objCount)
	for i := range objCount {
		bigObj[i] = testObject(smallSizeLimit * 2)
		smallObj[i] = testObject(smallSizeLimit / 2)
	}

	testGet := func(t *testing.T, s *shard.Shard, i int) {
		res1, err := s.Get(objectcore.AddressOf(smallObj[i]), true)
		require.NoError(t, err)
		require.Equal(t, smallObj[i], res1)

		res2, err := s.Get(objectcore.AddressOf(bigObj[i]), true)
		require.NoError(t, err)
		require.Equal(t, bigObj[i], res2)
	}

	testPut := func(t *testing.T, s *shard.Shard, i int) {
		err = s.Put(smallObj[i], nil)
		require.NoError(t, err)

		err = s.Put(bigObj[i], nil)
		require.NoError(t, err)
	}

	testHead := func(t *testing.T, s *shard.Shard, i int) {
		res1, err := s.Head(objectcore.AddressOf(smallObj[i]), false)
		require.NoError(t, err)
		require.Equal(t, smallObj[i].CutPayload(), res1)

		res2, err := s.Head(objectcore.AddressOf(bigObj[i]), false)
		require.NoError(t, err)
		require.Equal(t, bigObj[i].CutPayload(), res2)
	}

	// Put and Get uncompressed object
	s := newShardCompression(t, false)
	testPut(t, s, 0)
	testGet(t, s, 0)
	testHead(t, s, 0)
	require.NoError(t, s.Close())

	s = newShardCompression(t, true)
	testGet(t, s, 0) // get uncompressed object with compress enabled
	testHead(t, s, 0)
	testPut(t, s, 1)
	testGet(t, s, 1)
	require.NoError(t, s.Close())

	s = newShardCompression(t, false)
	testGet(t, s, 0) // get old uncompressed object
	testHead(t, s, 0)
	testGet(t, s, 1) // get compressed object with compression disabled
	testHead(t, s, 1)
	testPut(t, s, 2)
	testGet(t, s, 2)
	testHead(t, s, 2)
	require.NoError(t, s.Close())
}

func TestBlobstor_needsCompression(t *testing.T) {
	const smallSizeLimit = 512
	newShardCompression := func(t *testing.T, compress bool, ct ...string) *shard.Shard {
		dir, err := os.MkdirTemp("", "neofs*")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })

		return newCustomShard(t, dir, false, nil,
			shard.WithCompressObjects(compress),
			shard.WithUncompressableContentTypes(ct),
			shard.WithBlobstor(fstree.New(fstree.WithPath(dir))),
		)
	}

	newObjectWithCt := func(contentType string) *object.Object {
		obj := testObject(smallSizeLimit + 1)
		if contentType != "" {
			var a object.Attribute
			a.SetKey(object.AttributeContentType)
			a.SetValue(contentType)
			obj.SetAttributes(a)
		}
		return obj
	}

	t.Run("content-types specified", func(t *testing.T) {
		s := newShardCompression(t, true, "audio/*", "*/x-mpeg", "*/mpeg", "application/x-midi")

		obj := newObjectWithCt("video/mpeg")
		require.False(t, s.NeedsCompression(obj))

		obj = newObjectWithCt("audio/aiff")
		require.False(t, s.NeedsCompression(obj))

		obj = newObjectWithCt("application/x-midi")
		require.False(t, s.NeedsCompression(obj))

		obj = newObjectWithCt("text/plain")
		require.True(t, s.NeedsCompression(obj))

		obj = newObjectWithCt("")
		require.True(t, s.NeedsCompression(obj))
	})
	t.Run("content-types omitted", func(t *testing.T) {
		s := newShardCompression(t, true)
		obj := newObjectWithCt("video/mpeg")
		require.True(t, s.NeedsCompression(obj))
	})
	t.Run("compress disabled", func(t *testing.T) {
		s := newShardCompression(t, false, "video/mpeg")

		obj := newObjectWithCt("video/mpeg")
		require.False(t, s.NeedsCompression(obj))

		obj = newObjectWithCt("text/plain")
		require.False(t, s.NeedsCompression(obj))
	})
}

func testObject(sz uint64) *object.Object {
	raw := object.New()

	raw.SetID(oidtest.ID())
	raw.SetContainerID(cidtest.ID())

	raw.SetPayload(make([]byte, sz))

	// fit the binary size to the required
	data := raw.Marshal()
	if ln := uint64(len(data)); ln > sz {
		raw.SetPayload(raw.Payload()[:sz-(ln-sz)])
	}

	return raw
}
