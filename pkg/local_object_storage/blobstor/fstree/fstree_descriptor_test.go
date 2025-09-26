package fstree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFSTreeDescriptor_CreateAndValidate(t *testing.T) {
	dir := t.TempDir()

	fs1 := New(
		WithPath(dir),
		WithDepth(2),
		WithShardID("shard1"),
	)
	require.NoError(t, fs1.Init())
	desc := filepath.Join(dir, ".fstree.json")

	b, err := os.ReadFile(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{"version": 1,"depth": 2,"shard_id": "shard1"}`, string(b))

	t.Run("same config", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
			WithShardID("shard1"),
		)
		err = fs.Init()
		require.NoError(t, err)
	})

	t.Run("depth mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(3), // mismatch
			WithShardID("shard1"),
		)
		err = fs.Init()
		require.EqualError(t, err, "layout mismatch: on-disk depth=2, configured depth=3")
	})

	t.Run("shard ID mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
			WithShardID("shard2"), // mismatch
		)
		err = fs.Init()
		require.EqualError(t, err, "shard ID mismatch: on-disk shard ID=shard1, configured shard ID=shard2")
	})

	t.Run("version mismatch", func(t *testing.T) {
		data := []byte(`{"version":2,"depth":2,"shard_id":"shard1"}`) // version mismatch
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
			WithShardID("shard1"),
		)
		err = fs.Init()
		require.EqualError(t, err, "unsupported layout version: 2 (current version: 1)")
	})

	t.Run("invalid Json", func(t *testing.T) {
		require.NoError(t, os.WriteFile(desc, []byte("{invalid"), 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
			WithShardID("shard1"),
		)
		err = fs.Init()
		require.ErrorContains(t, err, "decode descriptor from JSON:")
	})

	t.Run("unknown fields", func(t *testing.T) {
		data := []byte(`{"version":1,"depth":2,"shard_id":"shard1","extra":42}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
			WithShardID("shard1"),
		)
		err = fs.Init()
		require.ErrorContains(t, err, "decode descriptor from JSON:")
		require.ErrorContains(t, err, "unknown field \"extra\"")
	})
}
