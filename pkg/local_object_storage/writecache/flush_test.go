package writecache

import (
	"os"
	"path/filepath"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/zap/zaptest"
)

func TestFlush(t *testing.T) {
	const (
		objCount  = 4
		smallSize = 256
	)

	type objectPair struct {
		addr oid.Address
		obj  *object.Object
	}

	newCache := func(t *testing.T) (Cache, *blobstor.BlobStor, *meta.DB) {
		dir := t.TempDir()
		mb := meta.New(
			meta.WithPath(filepath.Join(dir, "meta")),
			meta.WithEpochState(dummyEpoch{}))
		require.NoError(t, mb.Open(false))
		require.NoError(t, mb.Init())

		fsTree := fstree.New(
			fstree.WithPath(filepath.Join(dir, "blob")),
			fstree.WithDepth(0),
			fstree.WithDirNameLen(1))
		bs := blobstor.New(blobstor.WithStorages([]blobstor.SubStorage{
			{Storage: fsTree},
		}))
		require.NoError(t, bs.Open(false))
		require.NoError(t, bs.Init())

		wc := New(
			WithLogger(&logger.Logger{Logger: zaptest.NewLogger(t)}),
			WithPath(filepath.Join(dir, "writecache")),
			WithSmallObjectSize(smallSize),
			WithMetabase(mb),
			WithBlobstor(bs))
		require.NoError(t, wc.Open(false))
		require.NoError(t, wc.Init())

		// First set mode for metabase and blobstor to prevent background flushes.
		require.NoError(t, mb.SetMode(mode.ReadOnly))
		require.NoError(t, bs.SetMode(mode.ReadOnly))

		return wc, bs, mb
	}

	putObjects := func(t *testing.T, c Cache) []objectPair {
		objects := make([]objectPair, objCount)
		for i := range objects {
			obj, data := newObject(t, 1+(i%2)*smallSize)
			addr := objectCore.AddressOf(obj)

			var prm common.PutPrm
			prm.Address = objectCore.AddressOf(obj)
			prm.Object = obj
			prm.RawData = data

			_, err := c.Put(prm)
			require.NoError(t, err)

			objects[i] = objectPair{addr: addr, obj: obj}
		}
		return objects
	}

	check := func(t *testing.T, mb *meta.DB, bs *blobstor.BlobStor, objects []objectPair) {
		for i := range objects {
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

	t.Run("no errors", func(t *testing.T) {
		wc, bs, mb := newCache(t)
		objects := putObjects(t, wc)

		require.NoError(t, wc.SetMode(mode.ReadOnly))
		require.NoError(t, bs.SetMode(mode.ReadWrite))
		require.NoError(t, mb.SetMode(mode.ReadWrite))

		wc.(*cache).flushed.Add(objects[0].addr.EncodeToString(), true)
		wc.(*cache).flushed.Add(objects[1].addr.EncodeToString(), false)

		require.NoError(t, wc.Flush(false))

		for i := 0; i < 2; i++ {
			var mPrm meta.GetPrm
			mPrm.SetAddress(objects[i].addr)
			_, err := mb.Get(mPrm)
			require.Error(t, err)

			_, err = bs.Get(common.GetPrm{Address: objects[i].addr})
			require.Error(t, err)
		}

		check(t, mb, bs, objects[2:])
	})

	t.Run("flush on moving to degraded mode", func(t *testing.T) {
		wc, bs, mb := newCache(t)
		objects := putObjects(t, wc)

		// Blobstor is read-only, so we expect en error from `flush` here.
		require.Error(t, wc.SetMode(mode.Degraded))

		// First move to read-only mode to close background workers.
		require.NoError(t, wc.SetMode(mode.ReadOnly))
		require.NoError(t, bs.SetMode(mode.ReadWrite))
		require.NoError(t, mb.SetMode(mode.ReadWrite))

		wc.(*cache).flushed.Add(objects[0].addr.EncodeToString(), true)
		wc.(*cache).flushed.Add(objects[1].addr.EncodeToString(), false)

		require.NoError(t, wc.SetMode(mode.Degraded))

		for i := 0; i < 2; i++ {
			var mPrm meta.GetPrm
			mPrm.SetAddress(objects[i].addr)
			_, err := mb.Get(mPrm)
			require.Error(t, err)

			_, err = bs.Get(common.GetPrm{Address: objects[i].addr})
			require.Error(t, err)
		}

		check(t, mb, bs, objects[2:])
	})

	t.Run("ignore errors", func(t *testing.T) {
		testIgnoreErrors := func(t *testing.T, f func(*cache)) {
			wc, bs, mb := newCache(t)
			objects := putObjects(t, wc)
			f(wc.(*cache))

			require.NoError(t, wc.SetMode(mode.ReadOnly))
			require.NoError(t, bs.SetMode(mode.ReadWrite))
			require.NoError(t, mb.SetMode(mode.ReadWrite))

			require.Error(t, wc.Flush(false))
			require.NoError(t, wc.Flush(true))

			check(t, mb, bs, objects)
		}
		t.Run("db, invalid address", func(t *testing.T) {
			testIgnoreErrors(t, func(c *cache) {
				_, data := newObject(t, 1)
				require.NoError(t, c.db.Batch(func(tx *bbolt.Tx) error {
					b := tx.Bucket(defaultBucket)
					return b.Put([]byte{1, 2, 3}, data)
				}))
			})
		})
		t.Run("db, invalid object", func(t *testing.T) {
			testIgnoreErrors(t, func(c *cache) {
				require.NoError(t, c.db.Batch(func(tx *bbolt.Tx) error {
					b := tx.Bucket(defaultBucket)
					return b.Put([]byte(oidtest.Address().EncodeToString()), []byte{1, 2, 3})
				}))
			})
		})
		t.Run("fs, read error", func(t *testing.T) {
			testIgnoreErrors(t, func(c *cache) {
				obj, data := newObject(t, 1)

				var prm common.PutPrm
				prm.Address = objectCore.AddressOf(obj)
				prm.RawData = data

				_, err := c.fsTree.Put(prm)
				require.NoError(t, err)

				p := prm.Address.Object().EncodeToString() + "." + prm.Address.Container().EncodeToString()
				p = filepath.Join(c.fsTree.RootPath, p[:1], p[1:])

				_, err = os.Stat(p) // sanity check
				require.NoError(t, err)
				require.NoError(t, os.Chmod(p, 0))
			})
		})
		t.Run("fs, invalid object", func(t *testing.T) {
			testIgnoreErrors(t, func(c *cache) {
				var prm common.PutPrm
				prm.Address = oidtest.Address()
				prm.RawData = []byte{1, 2, 3}
				_, err := c.fsTree.Put(prm)
				require.NoError(t, err)
			})
		})
	})
}

func newObject(t *testing.T, size int) (*object.Object, []byte) {
	obj := object.New()
	ver := versionSDK.Current()

	obj.SetID(oidtest.ID())
	obj.SetOwnerID(usertest.ID())
	obj.SetContainerID(cidtest.ID())
	obj.SetType(object.TypeRegular)
	obj.SetVersion(&ver)
	obj.SetPayloadChecksum(checksumtest.Checksum())
	obj.SetPayloadHomomorphicHash(checksumtest.Checksum())
	obj.SetPayload(make([]byte, size))

	data, err := obj.Marshal()
	require.NoError(t, err)
	return obj, data
}

type dummyEpoch struct{}

func (dummyEpoch) CurrentEpoch() uint64 {
	return 0
}
