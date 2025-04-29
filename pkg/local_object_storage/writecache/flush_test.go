package writecache

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	checksumtest "github.com/nspcc-dev/neofs-sdk-go/checksum/test"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	versionSDK "github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

type objectPair struct {
	addr oid.Address
	obj  *object.Object
}

func TestFlush(t *testing.T) {
	const (
		objCount = 4
		bigSize  = defaultMaxBatchTreshold
	)

	newCache := func(t *testing.T, opts ...Option) (Cache, *blobstor.BlobStor, *meta.DB) {
		wc, bs, mb := newCache(t, append(opts, WithLogger(zaptest.NewLogger(t)))...)

		// First set mode for metabase and blobstor to prevent background flushes.
		require.NoError(t, mb.SetMode(mode.ReadOnly))
		require.NoError(t, bs.SetMode(mode.ReadOnly))

		return wc, bs, mb
	}

	putObjects := func(t *testing.T, c Cache) []objectPair {
		objects := make([]objectPair, objCount)
		for i := range objects {
			objects[i] = putObject(t, c, 1+(i%2)*bigSize)
		}
		return objects
	}

	check := func(t *testing.T, mb *meta.DB, bs *blobstor.BlobStor, objects []objectPair) {
		for i := range objects {
			res, err := bs.Get(objects[i].addr)
			require.NoError(t, err)
			require.Equal(t, objects[i].obj, res)
		}
	}

	t.Run("no errors", func(t *testing.T) {
		wc, bs, mb := newCache(t)
		defer wc.Close()
		objects := putObjects(t, wc)

		require.NoError(t, bs.SetMode(mode.ReadWrite))
		require.NoError(t, mb.SetMode(mode.ReadWrite))

		for _, obj := range objects {
			_, err := wc.Get(obj.addr)
			require.NoError(t, err)
		}

		require.NoError(t, wc.Flush(false))

		check(t, mb, bs, objects)
		require.Equal(t, wc.(*cache).objCounters.Size(), uint64(0))
		for _, obj := range objects {
			_, err := wc.Get(obj.addr)
			require.Error(t, err)
		}
	})

	t.Run("flush on moving to degraded mode", func(t *testing.T) {
		wc, bs, mb := newCache(t)
		defer wc.Close()
		objects := putObjects(t, wc)

		// Blobstor is read-only, so we expect en error from `flush` here.
		require.Error(t, wc.SetMode(mode.Degraded))

		// First move to read-only mode to close background workers.
		require.NoError(t, wc.SetMode(mode.ReadOnly))
		require.NoError(t, bs.SetMode(mode.ReadWrite))
		require.NoError(t, mb.SetMode(mode.ReadWrite))

		require.NoError(t, wc.SetMode(mode.Degraded))

		check(t, mb, bs, objects)
	})

	t.Run("ignore errors", func(t *testing.T) {
		testIgnoreErrors := func(t *testing.T, f func(*cache)) {
			var errCount atomic.Uint32
			wc, bs, mb := newCache(t, WithReportErrorFunc(func(message string, err error) {
				errCount.Add(1)
			}))
			defer wc.Close()
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
		t.Run("fs, read error", func(t *testing.T) {
			testIgnoreErrors(t, func(c *cache) {
				obj, data := newObject(t, 1)
				addr := objectCore.AddressOf(obj)

				err := c.fsTree.Put(addr, data)
				require.NoError(t, err)

				p := addr.Object().EncodeToString() + "." + addr.Container().EncodeToString()
				p = filepath.Join(c.fsTree.RootPath, p[:1], p[1:])

				_, err = os.Stat(p) // sanity check
				require.NoError(t, err)
				require.NoError(t, os.Chmod(p, 0))
			})
		})
		t.Run("fs, invalid object", func(t *testing.T) {
			testIgnoreErrors(t, func(c *cache) {
				err := c.fsTree.Put(oidtest.Address(), []byte{1, 2, 3})
				require.NoError(t, err)
			})
		})
	})

	t.Run("on init", func(t *testing.T) {
		wc, bs, mb := newCache(t)
		defer wc.Close()
		objects := []objectPair{
			// removed
			putObject(t, wc, 1),
			putObject(t, wc, bigSize+1),
			// not found
			putObject(t, wc, 1),
			putObject(t, wc, bigSize+1),
			// ok
			putObject(t, wc, 1),
			putObject(t, wc, bigSize+1),
		}

		require.NoError(t, wc.Close())
		require.NoError(t, bs.SetMode(mode.ReadWrite))
		require.NoError(t, mb.SetMode(mode.ReadWrite))

		for i := range objects {
			err := mb.Put(objects[i].obj, nil)
			require.NoError(t, err)
		}

		_, _, err := mb.Inhume(oidtest.Address(), 0, false, objects[0].addr, objects[1].addr)
		require.NoError(t, err)

		_, err = mb.Delete([]oid.Address{objects[2].addr, objects[3].addr})
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

func TestFlushPerformance(t *testing.T) {
	t.Skip()
	objectCounts := []int{100, 1000}
	workerCounts := []int{1, 4, 16}

	for _, objCount := range objectCounts {
		for _, workerCount := range workerCounts {
			t.Run(fmt.Sprintf("objects=%d_workers=%d", objCount, workerCount), func(t *testing.T) {
				t.Parallel()
				wc, bs, mb := newCache(t, WithFlushWorkersCount(workerCount))
				defer wc.Close()

				require.NoError(t, mb.SetMode(mode.ReadOnly))
				require.NoError(t, bs.SetMode(mode.ReadOnly))

				objects := make([]objectPair, objCount)
				for i := range objects {
					// some big objects and some small
					objects[i] = putObject(t, wc, 1+(i%2)*defaultMaxBatchTreshold)
				}
				for _, obj := range objects {
					_, err := wc.Get(obj.addr)
					require.NoError(t, err)
				}
				require.NoError(t, wc.Close())

				require.NoError(t, bs.SetMode(mode.ReadWrite))
				require.NoError(t, mb.SetMode(mode.ReadWrite))

				require.NoError(t, wc.Open(false))
				require.NoError(t, wc.Init())
				start := time.Now()
				waitForFlush(t, wc, objects)
				duration := time.Since(start)

				for i := range objects {
					res, err := bs.Get(objects[i].addr)
					require.NoError(t, err)
					require.Equal(t, objects[i].obj, res)
				}
				require.Equal(t, uint64(0), wc.(*cache).objCounters.Size())
				for _, obj := range objects {
					_, err := wc.Get(obj.addr)
					require.Error(t, err)
				}

				t.Logf("Flush took %v for %d objects with %d workers", duration, objCount, workerCount)
			})
		}
	}
}

func TestFlushErrorRetry(t *testing.T) {
	workerCounts := []int{1, 3, 16}

	for _, workerCount := range workerCounts {
		t.Run(fmt.Sprintf("worker=%d", workerCount), func(t *testing.T) {
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

			sub1 := &mockWriter{full: true, Storage: fsTree}
			bs := blobstor.New(
				blobstor.WithStorages(blobstor.SubStorage{Storage: sub1}),
				blobstor.WithCompressObjects(true))
			require.NoError(t, bs.Open(false))
			require.NoError(t, bs.Init())

			var logBuf safeBuffer
			multiSyncer := zapcore.NewMultiWriteSyncer(
				zapcore.Lock(zapcore.AddSync(os.Stderr)),
				zapcore.Lock(zapcore.AddSync(&logBuf)))
			logger := zap.New(
				zapcore.NewCore(
					zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
					multiSyncer,
					zapcore.DebugLevel,
				),
			)
			wc := New(WithPath(filepath.Join(dir, "writecache")),
				WithMetabase(mb),
				WithBlobstor(bs),
				WithFlushWorkersCount(workerCount),
				WithLogger(logger))
			require.NoError(t, wc.Open(false))
			require.NoError(t, wc.Init())

			defer wc.Close()

			objects := make([]objectPair, 5)
			for i := range objects {
				objects[i] = putObject(t, wc, 1024)
			}

			start := time.Now()
			go func() {
				time.Sleep(2 * time.Second)
				sub1.SetFull(false) // Allow to put objects
			}()

			waitForFlush(t, wc, objects)
			duration := time.Since(start)

			for i := range objects {
				res, err := bs.Get(objects[i].addr)
				require.NoError(t, err)
				require.Equal(t, objects[i].obj, res)
			}

			require.True(t, duration >= (defaultErrorDelay),
				"Flush completed too quickly (%v), expected at least %v retry delay",
				duration, (defaultErrorDelay))

			logOutput := logBuf.String()
			require.Contains(t, logOutput, "flush scheduler paused due to error")
			require.Contains(t, logOutput, common.ErrNoSpace.Error())

			require.Equal(t, uint64(0), wc.(*cache).objCounters.Size())
			t.Logf("Flush completed in %v after retrying", duration)
		})
	}
}

func TestFlushScheduler(t *testing.T) {
	wc, bs, mb := newCache(t)
	defer wc.Close()

	require.NoError(t, bs.SetMode(mode.ReadWrite))
	require.NoError(t, mb.SetMode(mode.ReadWrite))

	objects := make([]objectPair, 2)
	objects[0] = putObject(t, wc, 1)
	objects[1] = putObject(t, wc, defaultMaxBatchTreshold+1)

	for _, obj := range objects {
		_, err := wc.Get(obj.addr)
		require.NoError(t, err)
	}
	require.NoError(t, wc.Close())

	require.NoError(t, bs.SetMode(mode.ReadWrite))
	require.NoError(t, mb.SetMode(mode.ReadWrite))

	require.NoError(t, wc.Open(false))
	require.NoError(t, wc.Init())

	waitForFlush(t, wc, objects)

	for i := range objects {
		res, err := bs.Get(objects[i].addr)
		require.NoError(t, err)
		require.Equal(t, objects[i].obj, res)
	}
	require.Equal(t, uint64(0), wc.(*cache).objCounters.Size())
	for _, obj := range objects {
		_, err := wc.Get(obj.addr)
		require.Error(t, err)
	}
}

func waitForFlush(t *testing.T, wc Cache, objects []objectPair) {
	timeout := time.After(60 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			cachedCount := 0
			for _, obj := range objects {
				if _, err := wc.Get(obj.addr); err == nil {
					cachedCount++
				}
			}
			if cachedCount == 0 {
				return
			}
		case <-timeout:
			t.Fatalf("Flush did not complete within 60 seconds, %d objects still cached", len(objects))
		}
	}
}

func putObject(t *testing.T, c Cache, size int) objectPair {
	obj, data := newObject(t, size)

	err := c.Put(objectCore.AddressOf(obj), obj, data)
	require.NoError(t, err)

	return objectPair{objectCore.AddressOf(obj), obj}
}

func newObject(t *testing.T, size int) (*object.Object, []byte) {
	obj := object.New()
	ver := versionSDK.Current()

	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetContainerID(cidtest.ID())
	obj.SetType(object.TypeRegular)
	obj.SetVersion(&ver)
	obj.SetPayloadChecksum(checksumtest.Checksum())
	obj.SetPayloadHomomorphicHash(checksumtest.Checksum())
	obj.SetPayload(make([]byte, size))

	return obj, obj.Marshal()
}

type dummyEpoch struct{}

func (dummyEpoch) CurrentEpoch() uint64 {
	return 0
}

type mockWriter struct {
	common.Storage
	mu   sync.Mutex
	full bool
}

func (x *mockWriter) Put(addr oid.Address, data []byte) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.full {
		return common.ErrNoSpace
	}
	return x.Storage.Put(addr, data)
}

func (x *mockWriter) PutBatch(m map[oid.Address][]byte) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.full {
		return common.ErrNoSpace
	}
	return x.Storage.PutBatch(m)
}

func (x *mockWriter) SetFull(full bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.full = full
}

type safeBuffer struct {
	buf bytes.Buffer
	mu  sync.Mutex
}

func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
