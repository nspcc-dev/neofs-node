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
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/atomic"
	"go.uber.org/zap/zaptest"
)

type objectPair struct {
	addr oid.Address
	obj  *object.Object
}

func TestFlush(t *testing.T) {
	const (
		objCount  = 4
		smallSize = 256
	)

	newCache := func(t *testing.T, opts ...Option) (Cache, *blobstor.BlobStor, *meta.DB) {
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
			append([]Option{
				WithLogger(&logger.Logger{Logger: zaptest.NewLogger(t)}),
				WithPath(filepath.Join(dir, "writecache")),
				WithSmallObjectSize(smallSize),
				WithMetabase(mb),
				WithBlobstor(bs),
			}, opts...)...)
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
			objects[i] = putObject(t, c, 1+(i%2)*smallSize)
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
			var errCount atomic.Uint32
			wc, bs, mb := newCache(t, WithReportErrorFunc(func(message string, err error) {
				errCount.Inc()
			}))
			objects := putObjects(t, wc)
			f(wc.(*cache))

			require.NoError(t, wc.SetMode(mode.ReadOnly))
			require.NoError(t, bs.SetMode(mode.ReadWrite))
			require.NoError(t, mb.SetMode(mode.ReadWrite))

			require.Equal(t, uint32(0), errCount.Load())
			require.Error(t, wc.Flush(false))
			require.True(t, errCount.Load() > 0)
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

	t.Run("on init", func(t *testing.T) {
		wc, bs, mb := newCache(t)
		objects := []objectPair{
			// removed
			putObject(t, wc, 1),
			putObject(t, wc, smallSize+1),
			// not found
			putObject(t, wc, 1),
			putObject(t, wc, smallSize+1),
			// ok
			putObject(t, wc, 1),
			putObject(t, wc, smallSize+1),
		}

		require.NoError(t, wc.Close())
		require.NoError(t, bs.SetMode(mode.ReadWrite))
		require.NoError(t, mb.SetMode(mode.ReadWrite))

		for i := range objects {
			var prm meta.PutPrm
			prm.SetObject(objects[i].obj)
			_, err := mb.Put(prm)
			require.NoError(t, err)
		}

		var inhumePrm meta.InhumePrm
		inhumePrm.SetAddresses(objects[0].addr, objects[1].addr)
		inhumePrm.SetTombstoneAddress(oidtest.Address())
		_, err := mb.Inhume(inhumePrm)
		require.NoError(t, err)

		var deletePrm meta.DeletePrm
		deletePrm.SetAddresses(objects[2].addr, objects[3].addr)
		_, err = mb.Delete(deletePrm)
		require.NoError(t, err)

		require.NoError(t, bs.SetMode(mode.ReadOnly))
		require.NoError(t, mb.SetMode(mode.ReadOnly))

		// Open in read-only: no error, nothing is removed.
		require.NoError(t, wc.Open(true))
		require.NoError(t, wc.Init())
		for i := range objects {
			_, err := wc.Get(objects[i].addr)
			require.NoError(t, err, i)
		}
		require.NoError(t, wc.Close())

		// Open in read-write: no error, something is removed.
		require.NoError(t, wc.Open(false))
		require.NoError(t, wc.Init())
		for i := range objects {
			_, err := wc.Get(objects[i].addr)
			if i < 2 {
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound), i)
			} else {
				require.NoError(t, err, i)
			}
		}
	})
}

func putObject(t *testing.T, c Cache, size int) objectPair {
	obj, data := newObject(t, size)

	var prm common.PutPrm
	prm.Address = objectCore.AddressOf(obj)
	prm.Object = obj
	prm.RawData = data

	_, err := c.Put(prm)
	require.NoError(t, err)

	return objectPair{prm.Address, prm.Object}

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
