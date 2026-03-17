package fstree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/stretchr/testify/require"
)

func TestFSTreeDescriptor_CreateAndValidate(t *testing.T) {
	dir := t.TempDir()

	fs1 := New(
		WithPath(dir),
		WithDepth(2),
	)
	id1, err := common.NewID()
	require.NoError(t, err)
	fs1.SetShardID(id1)
	require.NoError(t, fs1.Init())
	desc := filepath.Join(dir, ".fstree.json")

	b, err := os.ReadFile(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{"version": 2,"depth": 2,"shard_id": "`+id1.String()+`"}`, string(b))

	t.Run("same config", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(id1)
		err = fs.Init()
		require.NoError(t, err)
	})

	t.Run("depth mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(3), // mismatch
		)
		fs.SetShardID(id1)
		err = fs.Init()
		require.EqualError(t, err, "layout mismatch: on-disk depth=2, configured depth=3")
	})

	t.Run("shard ID mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		id2, err := common.NewID()
		require.NoError(t, err)
		fs.SetShardID(id2) // mismatch
		err = fs.Init()
		require.EqualError(t, err, "shard ID mismatch: on-disk shard ID="+id1.String()+", configured shard ID="+id2.String())
	})

	t.Run("version mismatch", func(t *testing.T) {
		data := []byte(`{"version":3,"depth":2,"shard_id":"` + id1.String() + `"}`) // version mismatch
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(id1)
		err = fs.Init()
		require.EqualError(t, err, "unsupported layout version: 3 (current version: 2)")
	})

	t.Run("invalid Json", func(t *testing.T) {
		require.NoError(t, os.WriteFile(desc, []byte("{invalid"), 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(id1)
		err = fs.Init()
		require.ErrorContains(t, err, "decode descriptor from JSON:")
	})

	t.Run("unknown fields", func(t *testing.T) {
		data := []byte(`{"version":1,"depth":2,"shard_id":"` + id1.String() + `","extra":42}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		fs.SetShardID(id1)
		err = fs.Init()
		require.ErrorContains(t, err, "decode descriptor from JSON:")
		require.ErrorContains(t, err, "unknown field \"extra\"")
	})
}

func TestFSTreeDescriptor_MigrationFrom1Version(t *testing.T) {
	id1, err := common.NewID()
	require.NoError(t, err)

	id2, err := common.NewID()
	require.NoError(t, err)

	tests := []struct {
		name              string
		initialShardID    string
		configuredShardID common.ID
		expectedShardID   string
		checkMismatch     bool
	}{
		{
			name:              "path-based configured ID",
			initialShardID:    "/storage/fstree1",
			configuredShardID: id1,
			expectedShardID:   id1.String(),
			checkMismatch:     true,
		},
		{
			name:            "path-based without configured ID",
			initialShardID:  "/storage/fstree1",
			expectedShardID: "",
		},
		{
			name:              "empty shard ID configured ID",
			initialShardID:    "",
			configuredShardID: id1,
			expectedShardID:   id1.String(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			desc := filepath.Join(dir, ".fstree.json")

			data := []byte(`{"version":1,"depth":2,"shard_id":"` + tc.initialShardID + `"}`)
			require.NoError(t, os.WriteFile(desc, data, 0o600))

			fs := New(
				WithPath(dir),
				WithDepth(2),
			)
			if !tc.configuredShardID.IsZero() {
				fs.SetShardID(tc.configuredShardID)
			}
			require.NoError(t, fs.Init())
			require.NoError(t, fs.Close())

			b, err := os.ReadFile(desc)
			require.NoError(t, err)
			require.JSONEq(t, `{"version":2,"depth":2,"shard_id":"`+tc.expectedShardID+`"}`, string(b))

			if !tc.configuredShardID.IsZero() {
				fs2 := New(
					WithPath(dir),
					WithDepth(2),
				)
				fs2.SetShardID(tc.configuredShardID)
				require.NoError(t, fs2.Init())
				require.NoError(t, fs2.Close())
			}

			if tc.checkMismatch {
				fs3 := New(
					WithPath(dir),
					WithDepth(2),
				)
				fs3.SetShardID(id2)
				err = fs3.Init()
				require.EqualError(t, err, "shard ID mismatch: on-disk shard ID="+tc.expectedShardID+", configured shard ID="+id2.String())
			}
		})
	}
}
