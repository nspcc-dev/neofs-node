package writecache

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/storagetest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestGeneric(t *testing.T) {
	defer func() { _ = os.RemoveAll(t.Name()) }()

	var n int
	newCache := func(t *testing.T) storagetest.Component {
		n++
		dir := filepath.Join(t.Name(), strconv.Itoa(n))
		require.NoError(t, os.MkdirAll(dir, os.ModePerm))
		return New(
			WithLogger(zaptest.NewLogger(t)),
			WithPath(dir))
	}

	storagetest.TestAll(t, newCache)
}

func newCache(tb testing.TB, opts ...Option) (Cache, common.Storage) {
	dir := tb.TempDir()

	fsTree := fstree.New(
		fstree.WithPath(filepath.Join(dir, "fstree")),
		fstree.WithDepth(0))

	comp := &compression.Config{
		Enabled: true,
	}
	require.NoError(tb, comp.Init())
	fsTree.SetCompressor(comp)

	require.NoError(tb, fsTree.Open(false))
	require.NoError(tb, fsTree.Init())

	modeAwareStorage := NewModeAwareStorage(fsTree)

	wc := New(
		append([]Option{
			WithPath(filepath.Join(dir, "writecache")),
			WithStorage(modeAwareStorage),
		}, opts...)...)
	require.NoError(tb, wc.Open(false))
	require.NoError(tb, wc.Init())

	return wc, modeAwareStorage
}
