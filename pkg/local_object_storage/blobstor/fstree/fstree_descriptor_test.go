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
	fs1.SetShardID(mustShardID(t, "shard1"))
	require.NoError(t, fs1.Init())
	desc := filepath.Join(dir, ".fstree.json")

	b, err := os.ReadFile(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{"version": 2,"depth": 2,"shard_id": "shard1"}`, string(b))

	t.Run("same config", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(mustShardID(t, "shard1"))
		err = fs.Init()
		require.NoError(t, err)
	})

	t.Run("depth mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(3), // mismatch
		)
		fs.SetShardID(mustShardID(t, "shard1"))
		err = fs.Init()
		require.EqualError(t, err, "layout mismatch: on-disk depth=2, configured depth=3")
	})

	t.Run("shard ID mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(mustShardID(t, "shard2")) // mismatch
		err = fs.Init()
		require.EqualError(t, err, "shard ID mismatch: on-disk shard ID=shard1, configured shard ID=shard2")
	})

	t.Run("version mismatch", func(t *testing.T) {
		data := []byte(`{"version":3,"depth":2,"shard_id":"shard1"}`) // version mismatch
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(mustShardID(t, "shard1"))
		err = fs.Init()
		require.EqualError(t, err, "unsupported layout version: 3 (current version: 2)")
	})

	t.Run("invalid Json", func(t *testing.T) {
		require.NoError(t, os.WriteFile(desc, []byte("{invalid"), 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(mustShardID(t, "shard1"))
		err = fs.Init()
		require.ErrorContains(t, err, "decode descriptor from JSON:")
	})

	t.Run("unknown fields", func(t *testing.T) {
		data := []byte(`{"version":1,"depth":2,"shard_id":"shard1","extra":42}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(mustShardID(t, "shard1"))
		err = fs.Init()
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
	fs.SetShardID(mustShardID(t, "YZfSQhkAhFjXyyGReAEuTU"))
	require.NoError(t, fs.Init())
	require.NoError(t, fs.Close())

	b, err := os.ReadFile(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{"version":2,"depth":2,"shard_id":"YZfSQhkAhFjXyyGReAEuTU"}`, string(b))

	fs2 := New(
		WithPath(dir),
		WithDepth(2),
	)
	fs2.SetShardID(mustShardID(t, "YZfSQhkAhFjXyyGReAEuTU"))
	require.NoError(t, fs2.Init())
	require.NoError(t, fs2.Close())

	fs3 := New(
		WithPath(dir),
		WithDepth(2),
	)
	fs3.SetShardID(mustShardID(t, "APzY5YdK4SSf6N"))
	err = fs3.Init()
	require.EqualError(t, err, "shard ID mismatch: on-disk shard ID=YZfSQhkAhFjXyyGReAEuTU, configured shard ID=APzY5YdK4SSf6N")
}
