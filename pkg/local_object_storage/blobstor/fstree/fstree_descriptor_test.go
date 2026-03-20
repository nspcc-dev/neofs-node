package fstree

import (
	"os"
	"path/filepath"
	"testing"

	coreshard "github.com/nspcc-dev/neofs-node/pkg/core/shard"
	"github.com/stretchr/testify/require"
)

func mustShardID(t *testing.T, s string) *coreshard.ID {
	t.Helper()

	id, err := coreshard.DecodeString(s)
	require.NoError(t, err)
	return id
}

func TestFSTreeDescriptor_CreateAndValidate(t *testing.T) {
	dir := t.TempDir()

	fs1 := New(
		WithPath(dir),
		WithDepth(2),
	)
	require.NoError(t, fs1.Init(mustShardID(t, "shard1")))
	desc := filepath.Join(dir, ".fstree.json")

	b, err := os.ReadFile(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{"version": 2,"depth": 2,"shard_id": "shard1","subtype":"blobstor"}`, string(b))

	t.Run("same config", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(mustShardID(t, "shard1"))
		require.NoError(t, err)
	})

	t.Run("depth mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(3), // mismatch
		)
		err = fs.Init(mustShardID(t, "shard1"))
		require.EqualError(t, err, "layout mismatch: on-disk depth=2, configured depth=3")
	})

	t.Run("shard ID mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(mustShardID(t, "shard2")) // mismatch
		require.EqualError(t, err, "shard ID mismatch: on-disk shard ID=shard1, configured shard ID=shard2")
	})

	t.Run("subtype mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
			WithSubtype(SubtypeWriteCache),
		)
		err = fs.Init(mustShardID(t, "shard1"))
		require.EqualError(t, err, "subtype mismatch: on-disk subtype=blobstor, configured subtype=write-cache")
	})

	t.Run("version mismatch", func(t *testing.T) {
		data := []byte(`{"version":4,"depth":2,"shard_id":"shard1","subtype":"blobstor"}`) // version mismatch
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(mustShardID(t, "shard1"))
		require.EqualError(t, err, "unsupported layout version: 4 (current version: 2)")
	})

	t.Run("invalid Json", func(t *testing.T) {
		require.NoError(t, os.WriteFile(desc, []byte("{invalid"), 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(mustShardID(t, "shard1"))
		require.ErrorContains(t, err, "decode descriptor from JSON:")
	})

	t.Run("unknown fields", func(t *testing.T) {
		data := []byte(`{"version":1,"depth":2,"shard_id":"shard1","extra":42}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(mustShardID(t, "shard1"))
		require.ErrorContains(t, err, "decode descriptor from JSON:")
		require.ErrorContains(t, err, "unknown field \"extra\"")
	})
}

func TestFSTreeDescriptor_MigrationFrom1Version(t *testing.T) {
	dir := t.TempDir()
	desc := filepath.Join(dir, ".fstree.json")

	// old path-based ShardID
	data := []byte(`{"version":1,"depth":2,"shard_id":"/storage/fstree1"}`)
	require.NoError(t, os.WriteFile(desc, data, 0o600))

	fs := New(
		WithPath(dir),
		WithDepth(2),
	)
	require.NoError(t, fs.Init(mustShardID(t, "YZfSQhkAhFjXyyGReAEuTU")))
	require.NoError(t, fs.Close())

	b, err := os.ReadFile(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{"version":2,"depth":2,"shard_id":"YZfSQhkAhFjXyyGReAEuTU","subtype":"blobstor"}`, string(b))

	fs2 := New(
		WithPath(dir),
		WithDepth(2),
	)
	require.NoError(t, fs2.Init(mustShardID(t, "YZfSQhkAhFjXyyGReAEuTU")))
	require.NoError(t, fs2.Close())

	fs3 := New(
		WithPath(dir),
		WithDepth(2),
	)
	err = fs3.Init(mustShardID(t, "APzY5YdK4SSf6N"))
	require.EqualError(t, err, "shard ID mismatch: on-disk shard ID=YZfSQhkAhFjXyyGReAEuTU, configured shard ID=APzY5YdK4SSf6N")
}

func TestFSTree_ResolveShardID(t *testing.T) {
	t.Run("missing descriptor", func(t *testing.T) {
		dir := t.TempDir()
		fs := New(WithPath(dir))

		id, generated, err := fs.ResolveShardID()
		require.NoError(t, err)
		require.NotNil(t, id)
		require.True(t, generated)
	})

	t.Run("v1 descriptor with legacy path ID", func(t *testing.T) {
		dir := t.TempDir()
		desc := filepath.Join(dir, ".fstree.json")
		require.NoError(t, os.WriteFile(desc, []byte(`{"version":1,"depth":2,"shard_id":"/storage/fstree1"}`), 0o600))

		fs := New(WithPath(dir))

		id, generated, err := fs.ResolveShardID()
		require.NoError(t, err)
		require.NotNil(t, id)
		require.True(t, generated)
	})

	t.Run("v2 descriptor with valid ID", func(t *testing.T) {
		dir := t.TempDir()
		desc := filepath.Join(dir, ".fstree.json")
		require.NoError(t, os.WriteFile(desc, []byte(`{"version":2,"depth":2,"shard_id":"shard1","subtype":"blobstor"}`), 0o600))

		fs := New(WithPath(dir))

		id, generated, err := fs.ResolveShardID()
		require.NoError(t, err)
		require.NotNil(t, id)
		require.False(t, generated)
		require.Equal(t, "shard1", id.String())
	})
}
