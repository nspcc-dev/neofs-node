package writecache

import (
	"path/filepath"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestFlush(t *testing.T) {
	const (
		objCount  = 4
		smallSize = 256
	)

	dir := t.TempDir()
	mb := meta.New(
		meta.WithPath(filepath.Join(dir, "meta")),
		meta.WithEpochState(dummyEpoch{}))
	require.NoError(t, mb.Open(false))
	require.NoError(t, mb.Init())

	fsTree := fstree.New(fstree.WithPath(filepath.Join(dir, "blob")))
	bs := blobstor.New(blobstor.WithStorages([]blobstor.SubStorage{
		{Storage: fsTree},
	}))
	require.NoError(t, bs.Open(false))
	require.NoError(t, bs.Init())

	wc := New(
		WithLogger(zaptest.NewLogger(t)),
		WithPath(filepath.Join(dir, "writecache")),
		WithSmallObjectSize(smallSize),
		WithMetabase(mb),
		WithBlobstor(bs))
	require.NoError(t, wc.Open(false))
	require.NoError(t, wc.Init())

	// First set mode for metabase and blobstor to prevent background flushes.
	require.NoError(t, mb.SetMode(mode.ReadOnly))
	require.NoError(t, bs.SetMode(mode.ReadOnly))

	type objectPair struct {
		addr oid.Address
		obj  *object.Object
	}

	objects := make([]objectPair, objCount)
	for i := range objects {
		obj := object.New()
		ver := versionSDK.Current()

		obj.SetID(oidtest.ID())
		obj.SetOwnerID(usertest.ID())
		obj.SetContainerID(cidtest.ID())
		obj.SetType(object.TypeRegular)
		obj.SetVersion(&ver)
		obj.SetPayloadChecksum(checksumtest.Checksum())
		obj.SetPayloadHomomorphicHash(checksumtest.Checksum())
		obj.SetPayload(make([]byte, 1+(i%2)*smallSize))

		addr := objectCore.AddressOf(obj)
		data, err := obj.Marshal()
		require.NoError(t, err)

		var prm common.PutPrm
		prm.Address = objectCore.AddressOf(obj)
		prm.Object = obj
		prm.RawData = data

		_, err = wc.Put(prm)
		require.NoError(t, err)

		objects[i] = objectPair{addr: addr, obj: obj}
	}

	t.Run("must be read-only", func(t *testing.T) {
		require.ErrorIs(t, wc.Flush(), errMustBeReadOnly)
	})

	require.NoError(t, wc.SetMode(mode.ReadOnly))
	require.NoError(t, bs.SetMode(mode.ReadWrite))
	require.NoError(t, mb.SetMode(mode.ReadWrite))

	wc.(*cache).flushed.Add(objects[0].addr.EncodeToString(), true)
	wc.(*cache).flushed.Add(objects[1].addr.EncodeToString(), false)

	require.NoError(t, wc.Flush())

	for i := 0; i < 2; i++ {
		var mPrm meta.GetPrm
		mPrm.SetAddress(objects[i].addr)
		_, err := mb.Get(mPrm)
		require.Error(t, err)

		_, err = bs.Get(common.GetPrm{Address: objects[i].addr})
		require.Error(t, err)
	}

	for i := 2; i < objCount; i++ {
		var mPrm meta.StorageIDPrm
		mPrm.SetAddress(objects[i].addr)

		mRes, err := mb.StorageID(mPrm)
		require.NoError(t, err)

		var prm common.GetPrm
		prm.Address = objects[i].addr
		prm.StorageID = mRes.StorageID()

		res, err := bs.Get(prm)
		require.NoError(t, err)
		require.Equal(t, objects[i].obj, res.Object)
	}
}

type dummyEpoch struct{}

func (dummyEpoch) CurrentEpoch() uint64 {
	return 0
}
