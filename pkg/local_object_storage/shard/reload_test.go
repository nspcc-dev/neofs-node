package shard

import (
	"os"
	"path/filepath"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	sessiontest "github.com/nspcc-dev/neofs-sdk-go/session/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestShardReload(t *testing.T) {
	p := t.Name()
	defer os.RemoveAll(p)

	l := zaptest.NewLogger(t)
	blobOpts := []blobstor.Option{
		blobstor.WithLogger(l),
		blobstor.WithStorages([]blobstor.SubStorage{
			{
				Storage: fstree.New(
					fstree.WithPath(filepath.Join(p, "blob")),
					fstree.WithDepth(1)),
			},
		}),
	}

	metaOpts := []meta.Option{
		meta.WithPath(filepath.Join(p, "meta")),
		meta.WithEpochState(epochState{})}

	opts := []Option{
		WithLogger(l),
		WithBlobStorOptions(blobOpts...),
		WithMetaBaseOptions(metaOpts...),
		WithPiloramaOptions(
			pilorama.WithPath(filepath.Join(p, "pilorama")))}

	sh := New(opts...)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	objects := make([]objAddr, 5)
	for i := range objects {
		objects[i].obj = newObject(t)
		objects[i].addr = objectCore.AddressOf(objects[i].obj)
		require.NoError(t, putObject(sh, objects[i].obj))
	}

	checkHasObjects := func(t *testing.T, exists bool) {
		for i := range objects {
			res, err := sh.Exists(objects[i].addr, false)
			require.NoError(t, err)
			require.Equal(t, exists, res, "object #%d is missing", i)
		}
	}

	checkHasObjects(t, true)

	t.Run("same config, no-op", func(t *testing.T) {
		require.NoError(t, sh.Reload(opts...))
		checkHasObjects(t, true)
	})

	t.Run("open meta at new path", func(t *testing.T) {
		newShardOpts := func(metaPath string, resync bool) []Option {
			metaOpts := []meta.Option{meta.WithPath(metaPath), meta.WithEpochState(epochState{})}
			return append(opts, WithMetaBaseOptions(metaOpts...), WithResyncMetabase(resync))
		}

		newOpts := newShardOpts(filepath.Join(p, "meta1"), false)
		require.NoError(t, sh.Reload(newOpts...))

		checkHasObjects(t, false) // new path, but no resync

		t.Run("can put objects", func(t *testing.T) {
			obj := newObject(t)
			require.NoError(t, putObject(sh, obj))
			objects = append(objects, objAddr{obj: obj, addr: objectCore.AddressOf(obj)})
		})

		newOpts = newShardOpts(filepath.Join(p, "meta2"), true)
		require.NoError(t, sh.Reload(newOpts...))

		checkHasObjects(t, true) // all objects are restored, including the new one

		t.Run("reload failed", func(t *testing.T) {
			badPath := filepath.Join(p, "meta3")
			require.NoError(t, os.WriteFile(badPath, []byte{1}, 0))

			newOpts = newShardOpts(badPath, true)
			require.Error(t, sh.Reload(newOpts...))

			// Cleanup is done, no panic.
			obj := newObject(t)
			require.ErrorIs(t, putObject(sh, obj), ErrReadOnlyMode)

			// Old objects are still accessible.
			checkHasObjects(t, true)

			// Successive reload produces no undesired effects.
			require.NoError(t, os.RemoveAll(badPath))
			require.NoError(t, sh.Reload(newOpts...))

			obj = newObject(t)
			require.NoError(t, putObject(sh, obj))

			objects = append(objects, objAddr{obj: obj, addr: objectCore.AddressOf(obj)})
			checkHasObjects(t, true)
		})
	})
}

func putObject(sh *Shard, obj *objectSDK.Object) error {
	var prm PutPrm
	prm.SetObject(obj)

	_, err := sh.Put(prm)
	return err
}

func newObject(t testing.TB) *objectSDK.Object {
	x := objectSDK.New()
	ver := version.Current()

	x.SetID(oidtest.ID())
	tok := sessiontest.Object()
	x.SetSessionToken(&tok)
	x.SetPayload([]byte{1, 2, 3})
	x.SetPayloadSize(3)
	owner := usertest.ID()
	x.SetOwnerID(&owner)
	x.SetContainerID(cidtest.ID())
	x.SetType(objectSDK.TypeRegular)
	x.SetVersion(&ver)
	x.SetPayloadChecksum(checksumtest.Checksum())
	x.SetPayloadHomomorphicHash(checksumtest.Checksum())
	return x
}
