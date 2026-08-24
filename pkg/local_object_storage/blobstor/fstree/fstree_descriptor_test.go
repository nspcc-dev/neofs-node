package fstree

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
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
	require.JSONEq(t, `{"version": 4,"depth": 2,"shard_id": "`+id1.String()+`","subtype":"blobstor"}`, string(b))

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
		data := []byte(`{"version":5,"depth":2,"shard_id":"` + id1.String() + `","subtype":"blobstor"}`) // version mismatch
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(id1)
		require.EqualError(t, err, "unsupported layout version: 5 (current version: 4)")
	})

	t.Run("subtype mismatch", func(t *testing.T) {
		data := []byte(`{"version":4,"depth":2,"shard_id":"` + id1.String() + `","subtype":"blobstor"}`)
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

	t.Run("reshape depth", func(t *testing.T) {
		dir := t.TempDir()
		id, err := common.NewID()
		require.NoError(t, err)

		old := New(
			WithPath(dir),
			WithDepth(2),
		)
		require.NoError(t, old.Init(id))

		reshaped := New(
			WithPath(dir),
			WithDepth(3),
			WithAllowDepthChange(true),
		)
		require.NoError(t, reshaped.Init(id))

		<-reshaped.reshapeDone

		desc, err := os.ReadFile(filepath.Join(dir, ".fstree.json"))
		require.NoError(t, err)
		require.JSONEq(t, `{"version":4,"depth":3,"shard_id":"`+id.String()+`","subtype":"blobstor"}`, string(desc))

		t.Run("restart", func(t *testing.T) {
			fs := New(
				WithPath(dir),
				WithDepth(3),
			)
			require.NoError(t, fs.Init(id))
		})

		t.Run("wrong target depth", func(t *testing.T) {
			fs := New(
				WithPath(dir),
				WithDepth(4),
			)
			err := fs.Init(id)
			require.EqualError(t, err, "layout mismatch: on-disk depth=3, configured depth=4")
		})

		t.Run("read-only storage", func(t *testing.T) {
			readOnlyDir := t.TempDir()
			old := New(
				WithPath(readOnlyDir),
				WithDepth(2),
			)
			require.NoError(t, old.Init(id))

			fs := New(
				WithPath(readOnlyDir),
				WithDepth(3),
				WithAllowDepthChange(true),
			)
			require.NoError(t, fs.Open(true))
			require.EqualError(t, fs.Init(id), "can't reshape read-only storage")
		})
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
				require.JSONEq(t, `{"version":4,"depth":2,"shard_id":"`+tc.expectedShardID+`","subtype":"`+tc.subtype+`"}`, string(b))
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

func TestFSTreeDescriptor_ActiveReshape(t *testing.T) {
	dir := t.TempDir()
	id, err := common.NewID()
	require.NoError(t, err)

	old := New(WithPath(dir), WithDepth(2))
	require.NoError(t, old.Init(id))
	obj := objecttest.Object()
	require.NoError(t, old.Put(obj.Address(), obj.Marshal()))
	require.NoError(t, old.Close())

	require.NoError(t, writeDescriptor(filepath.Join(dir, ".fstree.json"), fsDescriptor{
		Version: currentVersion,
		Depth:   2,
		ShardID: id.String(),
		Subtype: SubtypeBlobstor,
		Reshape: &reshapeDescriptor{FromDepth: 2, ToDepth: 3},
	}))

	t.Run("read-only fallback", func(t *testing.T) {
		fs := New(WithPath(dir), WithDepth(3))
		require.NoError(t, fs.Open(true))
		require.NoError(t, fs.Init(id))

		got, err := fs.Get(obj.Address())
		require.NoError(t, err)
		require.Equal(t, obj.Marshal(), got.Marshal())
	})

	t.Run("rejects another target", func(t *testing.T) {
		fs := New(WithPath(dir), WithDepth(4))
		require.EqualError(t, fs.Init(id), "layout reshape target mismatch: on-disk target depth=3, configured depth=4")
	})
}

func TestFSTreeReshapeRecoveryFromCheckpoint(t *testing.T) {
	dir := t.TempDir()
	id, err := common.NewID()
	require.NoError(t, err)

	old := New(WithPath(dir), WithDepth(2), WithCombinedCountLimit(1))
	require.NoError(t, old.Init(id))
	addrs := make([]oid.Address, 3)
	for i := range addrs {
		obj := objecttest.Object()
		addrs[i] = obj.Address()
		require.NoError(t, old.Put(addrs[i], obj.Marshal()))
	}
	require.NoError(t, old.Close())

	sort.Slice(addrs, func(i, j int) bool {
		return old.treePath(addrs[i]) < old.treePath(addrs[j])
	})
	require.NoError(t, writeDescriptor(filepath.Join(dir, ".fstree.json"), fsDescriptor{
		Version: currentVersion,
		Depth:   2,
		ShardID: id.String(),
		Subtype: SubtypeBlobstor,
		Reshape: &reshapeDescriptor{FromDepth: 2, ToDepth: 3},
	}))

	interrupted := New(WithPath(dir), WithDepth(3), WithCombinedCountLimit(1))
	interrupted.secondaryDepth = 2
	require.NoError(t, interrupted.checkConfig())
	firstPath := old.treePath(addrs[0])
	moved, err := interrupted.reshapeFile(firstPath, addrs[0], new(bool))
	require.NoError(t, err)
	require.True(t, moved)
	relativePath, err := filepath.Rel(dir, firstPath)
	require.NoError(t, err)
	require.NoError(t, interrupted.updateReshapeProgress(filepath.ToSlash(relativePath)))

	resumed := New(WithPath(dir), WithDepth(3), WithCombinedCountLimit(1))
	require.NoError(t, resumed.Init(id))
	<-resumed.reshapeDone
	require.NoError(t, resumed.Close())

	for i := range addrs {
		_, err = os.Stat(resumed.treePath(addrs[i]))
		require.NoError(t, err)
		_, err = os.Stat(old.treePath(addrs[i]))
		require.ErrorIs(t, err, os.ErrNotExist)
	}

	desc, err := os.ReadFile(filepath.Join(dir, ".fstree.json"))
	require.NoError(t, err)
	require.JSONEq(t, `{"version":4,"depth":3,"shard_id":"`+id.String()+`","subtype":"blobstor"}`, string(desc))
}

func TestFSTreeDescriptor_ReshapeProgress(t *testing.T) {
	dir := t.TempDir()
	id, err := common.NewID()
	require.NoError(t, err)

	descPath := filepath.Join(dir, ".fstree.json")
	require.NoError(t, writeDescriptor(descPath, fsDescriptor{
		Version: currentVersion,
		Depth:   2,
		ShardID: id.String(),
		Subtype: SubtypeBlobstor,
		Reshape: &reshapeDescriptor{FromDepth: 2, ToDepth: 3},
	}))

	fs := New(WithPath(dir), WithDepth(3))
	fs.secondaryDepth = 2
	require.NoError(t, fs.checkConfig())
	require.NoError(t, fs.updateReshapeProgress("a/b/object"))

	progress, err := fs.reshapeLastProcessedPath()
	require.NoError(t, err)
	require.Equal(t, "a/b/object", progress)
}

func TestFSTreeDescriptor_MigrationFrom3Version(t *testing.T) {
	dir := t.TempDir()
	id, err := common.NewID()
	require.NoError(t, err)

	desc := filepath.Join(dir, ".fstree.json")
	data := []byte(`{"version":3,"depth":2,"shard_id":"` + id.String() + `","subtype":"blobstor"}`)
	require.NoError(t, os.WriteFile(desc, data, 0o600))

	fs := New(
		WithPath(dir),
		WithDepth(2),
		WithSubtype(SubtypeBlobstor),
	)
	require.NoError(t, fs.Init(id))

	b, err := os.ReadFile(desc)
	require.NoError(t, err)
	require.JSONEq(t, `{"version":4,"depth":2,"shard_id":"`+id.String()+`","subtype":"blobstor"}`, string(b))
}

func TestFSTreeDescriptor_MigrationFrom2Version(t *testing.T) {
	t.Run("add explicit subtype and keep shard id", func(t *testing.T) {
		for _, tc := range []string{SubtypeBlobstor, "write-cache"} {
			t.Run(tc, func(t *testing.T) {
				dir := t.TempDir()
				id, err := common.NewID()
				require.NoError(t, err)

				desc := filepath.Join(dir, ".fstree.json")
				data := []byte(`{"version":2,"depth":2,"shard_id":"` + id.String() + `"}`)
				require.NoError(t, os.WriteFile(desc, data, 0o600))

				fs := New(
					WithPath(dir),
					WithDepth(2),
					WithSubtype(tc),
				)
				require.NoError(t, fs.Init(id))

				b, err := os.ReadFile(desc)
				require.NoError(t, err)
				require.JSONEq(t, `{"version":4,"depth":2,"shard_id":"`+id.String()+`","subtype":"`+tc+`"}`, string(b))
			})
		}
	})

	t.Run("require explicit subtype", func(t *testing.T) {
		dir := t.TempDir()
		id, err := common.NewID()
		require.NoError(t, err)

		desc := filepath.Join(dir, ".fstree.json")
		data := []byte(`{"version":2,"depth":2,"shard_id":"` + id.String() + `"}`)
		require.NoError(t, os.WriteFile(desc, data, 0o600))

		fs := New(
			WithPath(dir),
			WithDepth(2),
		)
		err = fs.Init(id)
		require.EqualError(t, err, "can't migrate FSTree descriptor from v2 to v3 without explicit subtype")
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
			WithSubtype(SubtypeBlobstor),
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
			WithSubtype(SubtypeBlobstor),
		)
		err = fs.Init(id2)
		require.EqualError(t, err, "shard ID mismatch: on-disk shard ID="+id1.String()+", configured shard ID="+id2.String())
	})
}
