package writecache

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/storagetest"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
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

func TestOpenMigratesLegacyFSTree(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "writecache")
	id, err := common.NewID()
	require.NoError(t, err)

	legacy := fstree.New(
		fstree.WithPath(cachePath),
		fstree.WithPerm(os.ModePerm),
		fstree.WithDepth(1),
		fstree.WithSubtype(wcStorageType),
		fstree.WithCombinedCountLimit(1),
	)
	require.NoError(t, legacy.Open(false))
	require.NoError(t, legacy.Init(id))
	obj := objecttest.Object()
	require.NoError(t, legacy.Put(obj.Address(), obj.Marshal()))
	require.NoError(t, legacy.Close())

	name := obj.Address().Object().EncodeToString() + "." + obj.Address().Container().EncodeToString()
	blockerPath := filepath.Join(cachePath, name[:1], name[1:2])
	require.NoError(t, os.WriteFile(blockerPath, nil, 0o600))

	wc := New(
		WithPath(cachePath),
		WithFlushWorkersCount(0),
	).(*cache)
	require.NoError(t, wc.Open(false))
	require.NoError(t, wc.Init(id))
	t.Cleanup(func() { require.NoError(t, wc.Close()) })

	got, err := wc.GetBytes(obj.Address())
	require.NoError(t, err)
	require.Equal(t, obj.Marshal(), got)
	require.Equal(t, uint64(len(got)), wc.objCounters.Size())

	descPath := filepath.Join(cachePath, ".fstree.json")
	require.Eventually(t, func() bool {
		var desc struct {
			Reshape *json.RawMessage `json:"reshape"`
		}
		data, err := os.ReadFile(descPath)
		return err == nil && json.Unmarshal(data, &desc) == nil && desc.Reshape != nil
	}, time.Second, time.Millisecond)

	require.NoError(t, wc.SetMode(mode.ReadOnly))
	_, err = wc.GetBytes(obj.Address())
	require.NoError(t, err)

	require.NoError(t, os.Remove(blockerPath))
	require.NoError(t, wc.SetMode(mode.ReadWrite))
	_, err = wc.GetBytes(obj.Address())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		var desc struct {
			Depth   uint64           `json:"depth"`
			Reshape *json.RawMessage `json:"reshape"`
		}
		data, err := os.ReadFile(descPath)
		return err == nil && json.Unmarshal(data, &desc) == nil && desc.Depth == 2 && desc.Reshape == nil
	}, time.Second, time.Millisecond)
}

func newCache(tb testing.TB, opts ...Option) (Cache, common.Storage) {
	dir := tb.TempDir()

	fsTree := fstree.New(
		fstree.WithPath(filepath.Join(dir, "fstree")),
		fstree.WithDepth(0))

	require.NoError(tb, fsTree.Open(false))
	require.NoError(tb, fsTree.Init(common.ID{}))

	modeAwareStorage := NewModeAwareStorage(fsTree)

	wc := New(
		append([]Option{
			WithPath(filepath.Join(dir, "writecache")),
			WithStorage(modeAwareStorage),
		}, opts...)...)
	require.NoError(tb, wc.Open(false))
	require.NoError(tb, wc.Init(common.ID{}))

	return wc, modeAwareStorage
}
