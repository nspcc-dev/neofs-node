package shard

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	coreshard "github.com/nspcc-dev/neofs-node/pkg/core/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/stretchr/testify/require"
)

type testFSTreeDescriptor struct {
	Version int    `json:"version"`
	Depth   uint64 `json:"depth"`
	ShardID string `json:"shard_id"`
	Subtype string `json:"subtype"`
}

func testShardIDFromText(t *testing.T, s string) *coreshard.ID {
	id := coreshard.NewFromBytes([]byte(s))
	require.NotNil(t, id)
	return id
}

func readFSTreeDescriptor(t *testing.T, path string) testFSTreeDescriptor {
	var d testFSTreeDescriptor
	raw, err := os.ReadFile(filepath.Join(path, ".fstree.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &d))
	return d
}

func TestShardID(t *testing.T) {
	newShard := func(t *testing.T, fsPath, metaPath, wcPath string) *Shard {
		opts := []Option{
			WithBlobstor(fstree.New(
				fstree.WithPath(fsPath),
				fstree.WithDepth(1),
			)),
			WithMetaBaseOptions(
				meta.WithPath(metaPath),
				meta.WithEpochState(epochState{}),
			),
		}

		if wcPath != "" {
			opts = append(opts,
				WithWriteCache(true),
				WithWriteCacheOptions(
					writecache.WithPath(wcPath),
				),
			)
		}
		s, err := New(opts...)
		require.NoError(t, err)
		return s
	}

	t.Run("resolve ID for missing descriptor", func(t *testing.T) {
		dir := t.TempDir()

		s := newShard(t,
			filepath.Join(dir, "fstree"),
			filepath.Join(dir, "meta"),
			"",
		)
		require.NotNil(t, s.ID())
	})

	t.Run("prefer descriptor ID over metabase", func(t *testing.T) {
		dir := t.TempDir()
		fsPath := filepath.Join(dir, "fstree")
		metaPath := filepath.Join(dir, "meta")

		descID := testShardIDFromText(t, "descriptor-id-0001")
		metaID := testShardIDFromText(t, "metabase-id-00002")

		fs := fstree.New(
			fstree.WithPath(fsPath),
			fstree.WithDepth(1),
		)
		require.NoError(t, fs.Init(descID))
		require.NoError(t, fs.Close())

		mb := meta.New(
			meta.WithPath(metaPath),
			meta.WithEpochState(epochState{}),
		)
		require.NoError(t, mb.Open(false))
		require.NoError(t, mb.Init(nil))
		require.NoError(t, mb.WriteShardID(metaID.Bytes()))
		require.NoError(t, mb.Close())

		s := newShard(t, fsPath, metaPath, "")
		require.Equal(t, descID, s.ID())
	})

	t.Run("propagate generated ID to metabase and write-cache", func(t *testing.T) {
		dir := t.TempDir()
		fsPath := filepath.Join(dir, "fstree")
		metaPath := filepath.Join(dir, "meta")
		wcPath := filepath.Join(dir, "writecache")

		s := newShard(t, fsPath, metaPath, wcPath)
		require.NoError(t, s.Open())
		require.NoError(t, s.Init())
		t.Cleanup(func() { require.NoError(t, s.Close()) })

		shardID := s.ID()
		require.NotNil(t, shardID)

		metaID, err := s.metaBase.ReadShardID()
		require.NoError(t, err)
		require.Equal(t, shardID.Bytes(), metaID)

		blobDesc := readFSTreeDescriptor(t, fsPath)
		require.Equal(t, shardID.String(), blobDesc.ShardID)
		require.Equal(t, fstree.SubtypeBlobstor, blobDesc.Subtype)

		wcDesc := readFSTreeDescriptor(t, wcPath)
		require.Equal(t, shardID.String(), wcDesc.ShardID)
		require.Equal(t, fstree.SubtypeWriteCache, wcDesc.Subtype)
	})

	t.Run("persist metabase ID into generated storages", func(t *testing.T) {
		dir := t.TempDir()
		fsPath := filepath.Join(dir, "fstree")
		metaPath := filepath.Join(dir, "meta")
		wcPath := filepath.Join(dir, "writecache")

		metaID := testShardIDFromText(t, "metabase-id-00002")

		mb := meta.New(
			meta.WithPath(metaPath),
			meta.WithEpochState(epochState{}),
		)
		require.NoError(t, mb.Open(false))
		require.NoError(t, mb.Init(nil))
		require.NoError(t, mb.WriteShardID(metaID.Bytes()))
		require.NoError(t, mb.Close())

		s := newShard(t, fsPath, metaPath, wcPath)
		require.Equal(t, metaID, s.ID())

		require.NoError(t, s.Open())
		require.NoError(t, s.Init())
		t.Cleanup(func() { require.NoError(t, s.Close()) })

		blobDesc := readFSTreeDescriptor(t, fsPath)
		require.Equal(t, metaID.String(), blobDesc.ShardID)
		require.Equal(t, fstree.SubtypeBlobstor, blobDesc.Subtype)

		wcDesc := readFSTreeDescriptor(t, wcPath)
		require.Equal(t, metaID.String(), wcDesc.ShardID)
		require.Equal(t, fstree.SubtypeWriteCache, wcDesc.Subtype)
	})
}
