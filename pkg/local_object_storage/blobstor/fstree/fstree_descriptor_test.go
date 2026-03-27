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
	require.NoError(t, fs1.Init(id1))
	desc := filepath.Join(dir, ".fstree.json")

	b, err := os.ReadFile(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{"version": 3,"depth": 2,"shard_id": "`+id1.String()+`","subtype":"blobstor"}`, string(b))

	t.Run("same config", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(id1)
		require.NoError(t, err)
	})

	t.Run("depth mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(3), // mismatch
		)
		err = fs.Init(id1)
		require.EqualError(t, err, "layout mismatch: on-disk depth=2, configured depth=3")
	})

	t.Run("shard ID mismatch", func(t *testing.T) {
		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		id2, err := common.NewID()
		require.NoError(t, err)
		err = fs.Init(id2)
		require.EqualError(t, err, "shard ID mismatch: on-disk shard ID="+id1.String()+", configured shard ID="+id2.String())
	})

	t.Run("version mismatch", func(t *testing.T) {
		data := []byte(`{"version":4,"depth":2,"shard_id":"` + id1.String() + `","subtype":"blobstor"}`) // version mismatch
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(id1)
		require.EqualError(t, err, "unsupported layout version: 4 (current version: 3)")
	})

	t.Run("subtype mismatch", func(t *testing.T) {
		data := []byte(`{"version":3,"depth":2,"shard_id":"` + id1.String() + `","subtype":"blobstor"}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
			WithSubtype("write-cache"),
		)
		err = fs.Init(id1)
		require.EqualError(t, err, "subtype mismatch: on-disk subtype=blobstor, configured subtype=write-cache")
	})

	t.Run("invalid Json", func(t *testing.T) {
		require.NoError(t, os.WriteFile(desc, []byte("{invalid"), 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(id1)
		require.ErrorContains(t, err, "decode descriptor from JSON:")
	})

	t.Run("unknown fields", func(t *testing.T) {
		data := []byte(`{"version":1,"depth":2,"shard_id":"` + id1.String() + `","extra":42}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(id1)
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
		subtype           string
		checkMismatch     bool
	}{
		{
			name:              "path-based configured ID",
			initialShardID:    "/storage/fstree1",
			configuredShardID: id1,
			expectedShardID:   id1.String(),
			subtype:           "blobstor",
			checkMismatch:     true,
		},
		{
			name:            "path-based without configured ID",
			initialShardID:  "/storage/fstree1",
			expectedShardID: "generated",
			subtype:         "blobstor",
		},
		{
			name:              "empty shard ID configured ID",
			initialShardID:    "",
			configuredShardID: id1,
			expectedShardID:   id1.String(),
			subtype:           "blobstor",
		},
		{
			name:              "empty shard ID write-cache subtype",
			initialShardID:    "",
			configuredShardID: id1,
			expectedShardID:   id1.String(),
			subtype:           "write-cache",
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
				WithSubtype(tc.subtype),
			)
			require.NoError(t, fs.Init(tc.configuredShardID))
			require.NoError(t, fs.Close())

			b, err := os.ReadFile(desc)
			require.NoError(t, err)
			if tc.expectedShardID == "generated" {
				require.NotContains(t, string(b), `"shard_id":""`)
			} else {
				require.JSONEq(t, `{"version":3,"depth":2,"shard_id":"`+tc.expectedShardID+`","subtype":"`+tc.subtype+`"}`, string(b))
			}

			if !tc.configuredShardID.IsZero() {
				fs2 := New(
					WithPath(dir),
					WithDepth(2),
					WithSubtype(tc.subtype),
				)
				require.NoError(t, fs2.Init(tc.configuredShardID))
				require.NoError(t, fs2.Close())
			}

			if tc.checkMismatch {
				fs3 := New(
					WithPath(dir),
					WithDepth(2),
				)
				err = fs3.Init(id2)
				require.EqualError(t, err, "shard ID mismatch: on-disk shard ID="+tc.expectedShardID+", configured shard ID="+id2.String())
			}
		})
	}
}

func TestFSTreeDescriptor_MigrationFrom2Version(t *testing.T) {
	t.Run("add subtype and keep shard id", func(t *testing.T) {
		dir := t.TempDir()
		id, err := common.NewID()
		require.NoError(t, err)

		desc := filepath.Join(dir, ".fstree.json")
		data := []byte(`{"version":2,"depth":2,"shard_id":"` + id.String() + `"}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
			WithSubtype("write-cache"),
		)
		require.NoError(t, fs.Init(id))

		b, err := os.ReadFile(desc)
		require.NoError(t, err)
		require.JSONEq(t, `{"version":3,"depth":2,"shard_id":"`+id.String()+`","subtype":"write-cache"}`, string(b))
	})

	t.Run("validate depth after migration", func(t *testing.T) {
		dir := t.TempDir()
		id, err := common.NewID()
		require.NoError(t, err)

		desc := filepath.Join(dir, ".fstree.json")
		data := []byte(`{"version":2,"depth":2,"shard_id":"` + id.String() + `"}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(3),
		)
		err = fs.Init(id)
		require.EqualError(t, err, "layout mismatch: on-disk depth=2, configured depth=3")
	})

	t.Run("validate shard id after migration", func(t *testing.T) {
		dir := t.TempDir()
		id1, err := common.NewID()
		require.NoError(t, err)
		id2, err := common.NewID()
		require.NoError(t, err)

		desc := filepath.Join(dir, ".fstree.json")
		data := []byte(`{"version":2,"depth":2,"shard_id":"` + id1.String() + `"}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(id2)
		require.EqualError(t, err, "shard ID mismatch: on-disk shard ID="+id1.String()+", configured shard ID="+id2.String())
	})
}
