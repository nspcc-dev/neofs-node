package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestCommandArgs(t *testing.T) {
	t.Run("requires path or config", func(t *testing.T) {
		err := execute(nil, new(bytes.Buffer), new(bytes.Buffer))
		require.EqualError(t, err, `either "path" or "config" flag must be set`)
	})

	t.Run("rejects path and config together", func(t *testing.T) {
		err := execute([]string{"--" + pathFlag, t.TempDir(), "--" + configFlag, filepath.Join(t.TempDir(), "config.yml")}, new(bytes.Buffer), new(bytes.Buffer))
		require.EqualError(t, err, `"path" and "config" flags are mutually exclusive`)
	})
}

func TestCommandPath(t *testing.T) {
	t.Run("rejects missing root", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing")
		err := execute([]string{"--" + pathFlag, path}, new(bytes.Buffer), new(bytes.Buffer))
		require.ErrorIs(t, err, fs.ErrNotExist)

		_, err = os.Stat(path)
		require.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("requires descriptor", func(t *testing.T) {
		path := t.TempDir()
		err := execute([]string{"--" + pathFlag, path}, new(bytes.Buffer), new(bytes.Buffer))
		require.ErrorIs(t, err, fs.ErrNotExist)

		_, err = os.Stat(filepath.Join(path, ".fstree.json"))
		require.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("migrates v2 blobstor descriptor", func(t *testing.T) {
		dir := t.TempDir()
		id, err := common.NewID()
		require.NoError(t, err)

		desc := filepath.Join(dir, ".fstree.json")
		data := []byte(`{"version":2,"depth":2,"shard_id":"` + id.String() + `"}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		require.NoError(t, execute([]string{"--" + pathFlag, dir}, new(bytes.Buffer), new(bytes.Buffer)))

		b, err := os.ReadFile(desc)
		require.NoError(t, err)
		require.JSONEq(t, `{"version":3,"depth":2,"shard_id":"`+id.String()+`","subtype":"blobstor"}`, string(b))
	})

	t.Run("rewrites compressed objects", func(t *testing.T) {
		dir := t.TempDir()
		obj := commandTestObject()
		compressed := commandZstdCompress(t, obj.Marshal())

		tree := fstree.New(
			fstree.WithPath(dir),
			fstree.WithCombinedCountLimit(1),
		)
		require.NoError(t, tree.Open(false))
		require.NoError(t, tree.Init(common.ID{}))
		require.NoError(t, tree.Put(obj.Address(), compressed))
		require.NoError(t, tree.Close())

		var out bytes.Buffer
		require.NoError(t, execute([]string{"--" + pathFlag, dir}, &out, new(bytes.Buffer)))

		require.Contains(t, out.String(), "scanned: 1\n")
		require.Contains(t, out.String(), "compressed: 1\n")
		require.Contains(t, out.String(), "rewritten: 1\n")
	})
}

func TestCommandConfig(t *testing.T) {
	t.Run("rewrites compressed objects", func(t *testing.T) {
		dir := t.TempDir()
		obj := commandTestObject()
		compressed := commandZstdCompress(t, obj.Marshal())

		tree := fstree.New(
			fstree.WithPath(dir),
			fstree.WithPerm(0o600),
			fstree.WithDepth(2),
			fstree.WithSubtype(fstree.SubtypeBlobstor),
			fstree.WithCombinedCountLimit(1),
		)
		require.NoError(t, tree.Open(false))
		require.NoError(t, tree.Init(common.ID{}))
		require.NoError(t, tree.Put(obj.Address(), compressed))
		require.NoError(t, tree.Close())

		cfgPath := filepath.Join(t.TempDir(), "node.yml")
		cfg := fmt.Sprintf(`storage:
  shards:
    - blobstor:
        type: fstree
        path: %s
        perm: 0600
        depth: 2
        combined_count_limit: 1
`, dir)
		require.NoError(t, os.WriteFile(cfgPath, []byte(cfg), 0o600))

		var out bytes.Buffer
		require.NoError(t, execute([]string{"--" + configFlag, cfgPath}, &out, new(bytes.Buffer)))

		require.Contains(t, out.String(), "shard 0 ("+dir+"):\n")
		require.Contains(t, out.String(), "scanned: 1\n")
		require.Contains(t, out.String(), "compressed: 1\n")
		require.Contains(t, out.String(), "rewritten: 1\n")
	})

	t.Run("skips unsupported blobstor shards", func(t *testing.T) {
		dir := t.TempDir()
		obj := commandTestObject()
		compressed := commandZstdCompress(t, obj.Marshal())

		tree := fstree.New(
			fstree.WithPath(dir),
			fstree.WithPerm(0o600),
			fstree.WithDepth(2),
			fstree.WithSubtype(fstree.SubtypeBlobstor),
			fstree.WithCombinedCountLimit(1),
		)
		require.NoError(t, tree.Open(false))
		require.NoError(t, tree.Init(common.ID{}))
		require.NoError(t, tree.Put(obj.Address(), compressed))
		require.NoError(t, tree.Close())

		unsupportedDir := filepath.Join(t.TempDir(), "peapod")
		cfgPath := filepath.Join(t.TempDir(), "node.yml")
		cfg := fmt.Sprintf(`storage:
  shards:
    - blobstor:
        type: peapod
        path: %s
    - blobstor:
        type: fstree
        path: %s
        perm: 0600
        depth: 2
        combined_count_limit: 1
`, unsupportedDir, dir)
		require.NoError(t, os.WriteFile(cfgPath, []byte(cfg), 0o600))

		var out bytes.Buffer
		require.NoError(t, execute([]string{"--" + configFlag, cfgPath}, &out, new(bytes.Buffer)))

		require.Contains(t, out.String(), "shard 0 ("+unsupportedDir+"): skipped unsupported blobstor type peapod\n")
		require.Contains(t, out.String(), "shard 1 ("+dir+"):\n")
		require.Contains(t, out.String(), "compressed: 1\n")
		require.Contains(t, out.String(), "rewritten: 1\n")
	})
}

func commandTestObject() object.Object {
	obj := objecttest.Object()
	payload := testutil.RandByteSlice(1024)
	obj.SetPayload(payload)
	obj.SetPayloadSize(uint64(len(payload)))
	return obj
}

func commandZstdCompress(t *testing.T, data []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	t.Cleanup(func() { enc.Close() })
	return enc.EncodeAll(data, nil)
}
