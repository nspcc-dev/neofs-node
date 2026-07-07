package fstree

import (
	"bytes"
	"io/fs"
	"os"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestFSTreeRewriteCompressed(t *testing.T) {
	t.Run("rewrites compressed object", func(t *testing.T) {
		tree, obj, compressed := putCompressedRewriteObject(t, WithCombinedCountLimit(1))
		require.True(t, isCompressed(readRawObjectFile(t, tree, obj.Address())))

		st, err := tree.RewriteCompressed()
		require.NoError(t, err)
		require.EqualValues(t, 1, st.Scanned)
		require.EqualValues(t, 1, st.Compressed)
		require.EqualValues(t, 1, st.Rewritten)
		require.EqualValues(t, 0, st.Skipped)
		require.EqualValues(t, len(compressed), st.CompressedBytes)
		require.EqualValues(t, len(obj.Marshal()), st.PlainBytes)

		raw := readRawObjectFile(t, tree, obj.Address())
		require.False(t, isCompressed(raw))
		require.Equal(t, obj.Marshal(), raw)
	})

	t.Run("preserves file mode", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("chmod semantics are platform-specific")
		}

		tree, obj, _ := putCompressedRewriteObject(t, WithCombinedCountLimit(1), WithPerm(0o600))
		p := tree.treePath(obj.Address())
		require.NoError(t, os.Chmod(p, 0o640))

		st, err := tree.RewriteCompressed()
		require.NoError(t, err)
		require.EqualValues(t, 1, st.Rewritten)

		info, err := os.Stat(p)
		require.NoError(t, err)
		require.Equal(t, fs.FileMode(0o640), info.Mode().Perm())
	})

	t.Run("skips uncompressed object", func(t *testing.T) {
		tree := newRewriteCompressedTree(t, WithCombinedCountLimit(1))
		obj := rewriteCompressedObject()

		require.NoError(t, tree.Put(obj.Address(), obj.Marshal()))

		st, err := tree.RewriteCompressed()
		require.NoError(t, err)
		require.EqualValues(t, 1, st.Scanned)
		require.EqualValues(t, 0, st.Compressed)
		require.EqualValues(t, 0, st.Rewritten)
		require.EqualValues(t, 1, st.Skipped)
		require.Equal(t, obj.Marshal(), readRawObjectFile(t, tree, obj.Address()))
	})

	t.Run("malformed object", func(t *testing.T) {
		tree := newRewriteCompressedTree(t, WithCombinedCountLimit(1))
		obj := rewriteCompressedObject()
		malformed := []byte{0x28, 0xb5, 0x2f, 0xfd, 1, 2, 3}

		require.NoError(t, tree.Put(obj.Address(), malformed))

		st, err := tree.RewriteCompressed()
		require.Error(t, err)
		require.EqualValues(t, 1, st.Failed)
		require.Equal(t, malformed, readRawObjectFile(t, tree, obj.Address()))
	})

	t.Run("rewrites combined hard-linked file", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("combined FSTree writer is Linux-only")
		}

		tree := newRewriteCompressedTree(t)
		obj1 := rewriteCompressedObject()
		obj2 := rewriteCompressedObject()
		obj3 := rewriteCompressedObject()

		batch := map[oid.Address][]byte{
			obj1.Address(): zstdCompress(t, obj1.Marshal()),
			obj2.Address(): zstdCompress(t, obj2.Marshal()),
			obj3.Address(): obj3.Marshal(),
		}
		require.NoError(t, tree.PutBatch(batch))

		info1 := statRawObjectFile(t, tree, obj1.Address())
		info2 := statRawObjectFile(t, tree, obj2.Address())
		require.True(t, os.SameFile(info1, info2))

		// Simulate the storage node appending the next combined entry.
		f, err := os.OpenFile(tree.treePath(obj1.Address()), os.O_WRONLY|os.O_APPEND, 0)
		require.NoError(t, err)
		_, err = f.Write([]byte{combinedPrefix, 0})
		require.NoError(t, err)
		require.NoError(t, f.Close())

		st, err := tree.RewriteCompressed()
		require.NoError(t, err)
		require.EqualValues(t, 3, st.Scanned)
		require.EqualValues(t, 2, st.Compressed)
		require.EqualValues(t, 2, st.Rewritten)
		require.EqualValues(t, 1, st.Skipped)

		info1 = statRawObjectFile(t, tree, obj1.Address())
		info2 = statRawObjectFile(t, tree, obj2.Address())
		require.False(t, os.SameFile(info1, info2))
		for _, obj := range []object.Object{obj1, obj2} {
			_, combined, err := compressedEntryForAddress(readRawObjectFile(t, tree, obj.Address()), obj.Address().Object())
			require.NoError(t, err)
			require.False(t, combined)
		}
		_, combined, err := compressedEntryForAddress(readRawObjectFile(t, tree, obj3.Address()), obj3.Address().Object())
		require.NoError(t, err)
		require.True(t, combined)

		assertFSTreeObjectBytes(t, tree, obj1)
		assertFSTreeObjectBytes(t, tree, obj2)
		assertFSTreeObjectBytes(t, tree, obj3)
	})
}

func newRewriteCompressedTree(t *testing.T, opts ...Option) *FSTree {
	opts = append([]Option{WithPath(t.TempDir())}, opts...)
	tree := New(opts...)
	require.NoError(t, tree.Open(false))
	require.NoError(t, tree.Init(common.ID{}))

	t.Cleanup(func() { require.NoError(t, tree.Close()) })
	return tree
}

func TestRewriteCompressedObjectFile(t *testing.T) {
	t.Run("does not restore deleted target", func(t *testing.T) {
		tree, obj, _ := putCompressedRewriteObject(t, WithCombinedCountLimit(1))
		path := tree.treePath(obj.Address())
		info := statRawObjectFile(t, tree, obj.Address())
		owner, err := newFileOwner(path, info)
		require.NoError(t, err)

		require.NoError(t, os.Remove(path))
		err = rewriteCompressedObjectFile(path, info, owner, obj.Marshal(), true)
		require.ErrorIs(t, err, errRewriteCompressedObjectGone)
		_, err = os.Stat(path)
		require.ErrorIs(t, err, fs.ErrNotExist)
	})

	t.Run("replaces changed target on Linux", func(t *testing.T) {
		tree, obj, _ := putCompressedRewriteObject(t, WithCombinedCountLimit(1))
		path := tree.treePath(obj.Address())
		info := statRawObjectFile(t, tree, obj.Address())
		owner, err := newFileOwner(path, info)
		require.NoError(t, err)

		replacement := []byte("new object data")
		replacementPath := path + ".replacement"
		require.NoError(t, os.WriteFile(replacementPath, replacement, 0o600))
		require.NoError(t, os.Rename(replacementPath, path))

		err = rewriteCompressedObjectFile(path, info, owner, obj.Marshal(), true)
		if runtime.GOOS == "linux" {
			require.NoError(t, err)
			require.Equal(t, obj.Marshal(), readRawObjectFile(t, tree, obj.Address()))
		} else {
			require.ErrorIs(t, err, errRewriteCompressedObjectReplaced)
			require.Equal(t, replacement, readRawObjectFile(t, tree, obj.Address()))
		}
	})
}

func putCompressedRewriteObject(t *testing.T, opts ...Option) (*FSTree, object.Object, []byte) {
	tree := newRewriteCompressedTree(t, opts...)
	obj := rewriteCompressedObject()
	compressed := zstdCompress(t, obj.Marshal())
	require.NoError(t, tree.Put(obj.Address(), compressed))
	return tree, obj, compressed
}

func rewriteCompressedObject() object.Object {
	obj := objecttest.Object()
	payload := testutil.RandByteSlice(1024)
	obj.SetPayload(payload)
	obj.SetPayloadSize(uint64(len(payload)))
	return obj
}

func zstdCompress(t *testing.T, data []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	t.Cleanup(func() { enc.Close() })
	return enc.EncodeAll(data, nil)
}

func readRawObjectFile(t *testing.T, tree *FSTree, addr oid.Address) []byte {
	data, err := os.ReadFile(tree.treePath(addr))
	require.NoError(t, err)
	return data
}

func statRawObjectFile(t *testing.T, tree *FSTree, addr oid.Address) os.FileInfo {
	info, err := os.Stat(tree.treePath(addr))
	require.NoError(t, err)
	return info
}

func assertFSTreeObjectBytes(t *testing.T, tree *FSTree, obj object.Object) {
	data, err := tree.GetBytes(obj.Address())
	require.NoError(t, err)
	require.True(t, bytes.Equal(obj.Marshal(), data))
}
