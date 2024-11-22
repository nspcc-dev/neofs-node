package shard_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestDump(t *testing.T) {
	t.Run("without write-cache", func(t *testing.T) {
		testDump(t, 10, false)
	})
	t.Run("with write-cache", func(t *testing.T) {
		// Put a bit more objects to write-cache to facilitate race-conditions.
		testDump(t, 100, true)
	})
}

func testDump(t *testing.T, objCount int, hasWriteCache bool) {
	const (
		wcSmallObjectSize = 1024          // 1 KiB, goes to write-cache memory
		wcBigObjectSize   = 4 * 1024      // 4 KiB, goes to write-cache FSTree
		bsSmallObjectSize = 10 * 1024     // 10 KiB, goes to peapod DB
		bsBigObjectSize   = 1024*1024 + 1 // > 1 MiB, goes to FSTree
	)

	var sh *shard.Shard
	if !hasWriteCache {
		sh = newShard(t, false)
	} else {
		sh = newCustomShard(t, t.TempDir(), true,
			[]writecache.Option{
				writecache.WithSmallObjectSize(wcSmallObjectSize),
				writecache.WithMaxObjectSize(wcBigObjectSize),
				writecache.WithLogger(zaptest.NewLogger(t)),
			},
			nil)
	}
	defer releaseShard(sh, t)

	out := filepath.Join(t.TempDir(), "dump")
	f, err := os.Create(out)
	require.NoError(t, err)

	t.Run("must be read-only", func(t *testing.T) {
		_, err := sh.Dump(f, false)
		require.NoError(t, f.Close())
		require.ErrorIs(t, err, shard.ErrMustBeReadOnly)
	})

	require.NoError(t, sh.SetMode(mode.ReadOnly))

	outEmpty := out + ".empty"
	f, err = os.Create(outEmpty)
	require.NoError(t, err)

	res, err := sh.Dump(f, false)
	require.NoError(t, f.Close())
	require.NoError(t, err)
	require.Equal(t, 0, res)
	require.NoError(t, sh.SetMode(mode.ReadWrite))

	// Approximate object header size.
	const headerSize = 400

	objects := make([]*objectSDK.Object, objCount)
	for i := range objCount {
		cnr := cidtest.ID()
		var size int
		switch i % 6 {
		case 0, 1:
			size = wcSmallObjectSize - headerSize
		case 2, 3:
			size = bsSmallObjectSize - headerSize
		case 4:
			size = wcBigObjectSize - headerSize
		default:
			size = bsBigObjectSize - headerSize
		}
		data := make([]byte, size)
		_, _ = rand.Read(data)
		obj := generateObjectWithPayload(cnr, data)
		objects[i] = obj

		err := sh.Put(objects[i], nil, 0)
		require.NoError(t, err)
	}

	require.NoError(t, sh.SetMode(mode.ReadOnly))

	f, err = os.Create(out)
	require.NoError(t, err)
	res, err = sh.Dump(f, false)
	require.NoError(t, f.Close())
	require.NoError(t, err)
	require.Equal(t, objCount, res)

	t.Run("restore", func(t *testing.T) {
		sh := newShard(t, false)
		defer releaseShard(sh, t)

		t.Run("empty dump", func(t *testing.T) {
			var restorePrm shard.RestorePrm
			restorePrm.WithPath(outEmpty)
			res, err := sh.Restore(restorePrm)
			require.NoError(t, err)
			require.Equal(t, 0, res.Count())
		})

		t.Run("invalid path", func(t *testing.T) {
			_, err := sh.Restore(*new(shard.RestorePrm))
			require.ErrorIs(t, err, os.ErrNotExist)
		})

		t.Run("invalid file", func(t *testing.T) {
			t.Run("invalid magic", func(t *testing.T) {
				out := out + ".wrongmagic"
				require.NoError(t, os.WriteFile(out, []byte{0, 0, 0, 0}, os.ModePerm))

				var restorePrm shard.RestorePrm
				restorePrm.WithPath(out)

				_, err := sh.Restore(restorePrm)
				require.ErrorIs(t, err, shard.ErrInvalidMagic)
			})

			fileData, err := os.ReadFile(out)
			require.NoError(t, err)

			t.Run("incomplete size", func(t *testing.T) {
				out := out + ".wrongsize"
				fileData := append(fileData, 1)
				require.NoError(t, os.WriteFile(out, fileData, os.ModePerm))

				var restorePrm shard.RestorePrm
				restorePrm.WithPath(out)

				_, err := sh.Restore(restorePrm)
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			})
			t.Run("incomplete object data", func(t *testing.T) {
				out := out + ".wrongsize"
				fileData := append(fileData, 1, 0, 0, 0)
				require.NoError(t, os.WriteFile(out, fileData, os.ModePerm))

				var restorePrm shard.RestorePrm
				restorePrm.WithPath(out)

				_, err := sh.Restore(restorePrm)
				require.ErrorIs(t, err, io.EOF)
			})
			t.Run("invalid object", func(t *testing.T) {
				out := out + ".wrongobj"
				fileData := append(fileData, 1, 0, 0, 0, 0xFF, 4, 0, 0, 0, 1, 2, 3, 4)
				require.NoError(t, os.WriteFile(out, fileData, os.ModePerm))

				var restorePrm shard.RestorePrm
				restorePrm.WithPath(out)

				_, err := sh.Restore(restorePrm)
				require.Error(t, err)

				t.Run("skip errors", func(t *testing.T) {
					sh := newCustomShard(t, filepath.Join(t.TempDir(), "ignore"), false, nil, nil)
					t.Cleanup(func() { require.NoError(t, sh.Close()) })

					var restorePrm shard.RestorePrm
					restorePrm.WithPath(out)
					restorePrm.WithIgnoreErrors(true)

					res, err := sh.Restore(restorePrm)
					require.NoError(t, err)
					require.Equal(t, objCount, res.Count())
					require.Equal(t, 2, res.FailCount())
				})
			})
		})

		var prm shard.RestorePrm
		prm.WithPath(out)
		t.Run("must allow write", func(t *testing.T) {
			require.NoError(t, sh.SetMode(mode.ReadOnly))

			_, err := sh.Restore(prm)
			require.ErrorIs(t, err, shard.ErrReadOnlyMode)
		})

		require.NoError(t, sh.SetMode(mode.ReadWrite))

		checkRestore(t, sh, prm, objects)
	})
}

func TestStream(t *testing.T) {
	sh1 := newCustomShard(t, filepath.Join(t.TempDir(), "shard1"), false, nil, nil)
	defer releaseShard(sh1, t)

	sh2 := newCustomShard(t, filepath.Join(t.TempDir(), "shard2"), false, nil, nil)
	defer releaseShard(sh2, t)

	const objCount = 5
	objects := make([]*objectSDK.Object, objCount)
	for i := range objCount {
		cnr := cidtest.ID()
		obj := generateObjectWithCID(cnr)
		objects[i] = obj

		err := sh1.Put(objects[i], nil, 0)
		require.NoError(t, err)
	}

	require.NoError(t, sh1.SetMode(mode.ReadOnly))

	r, w := io.Pipe()
	finish := make(chan struct{})

	go func() {
		res, err := sh1.Dump(w, false)
		require.NoError(t, err)
		require.Equal(t, objCount, res)
		require.NoError(t, w.Close())
		close(finish)
	}()

	var restorePrm shard.RestorePrm
	restorePrm.WithStream(r)

	checkRestore(t, sh2, restorePrm, objects)
	require.Eventually(t, func() bool {
		select {
		case <-finish:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func checkRestore(t *testing.T, sh *shard.Shard, prm shard.RestorePrm, objects []*objectSDK.Object) {
	res, err := sh.Restore(prm)
	require.NoError(t, err)
	require.Equal(t, len(objects), res.Count())

	for i := range objects {
		res, err := sh.Get(object.AddressOf(objects[i]), false)
		require.NoError(t, err)
		require.Equal(t, objects[i], res)
	}
}

func TestDumpIgnoreErrors(t *testing.T) {
	const (
		wcSmallObjectSize = 512                    // goes to write-cache memory
		wcBigObjectSize   = wcSmallObjectSize << 1 // goes to write-cache FSTree
		bsSmallObjectSize = wcSmallObjectSize << 2 // goes to peapod DB

		objCount   = 10
		headerSize = 400
	)

	dir := t.TempDir()
	bsPath := filepath.Join(dir, "blob")
	bsOpts := func(sw uint64) []blobstor.Option {
		return []blobstor.Option{
			blobstor.WithCompressObjects(true),
			blobstor.WithStorages([]blobstor.SubStorage{
				{
					Storage: peapod.New(filepath.Join(bsPath, "peapod.db"), 0o600, 10*time.Millisecond),
					Policy: func(_ *objectSDK.Object, data []byte) bool {
						return len(data) < bsSmallObjectSize
					},
				},
				{
					Storage: fstree.New(
						fstree.WithPath(bsPath),
						fstree.WithDepth(1)),
				},
			}),
		}
	}
	wcPath := filepath.Join(dir, "writecache")
	wcOpts := []writecache.Option{
		writecache.WithPath(wcPath),
		writecache.WithSmallObjectSize(wcSmallObjectSize),
		writecache.WithMaxObjectSize(wcBigObjectSize),
	}
	sh := newCustomShard(t, dir, true, wcOpts, bsOpts(2))

	objects := make([]*objectSDK.Object, objCount)
	for i := range objCount {
		size := (wcSmallObjectSize << (i % 4)) - headerSize
		obj := generateObjectWithPayload(cidtest.ID(), make([]byte, size))
		objects[i] = obj

		err := sh.Put(objects[i], nil, 0)
		require.NoError(t, err)
	}

	releaseShard(sh, t)

	b := bytes.NewBuffer(nil)
	badObject := make([]byte, 1000)
	enc, err := zstd.NewWriter(b)
	require.NoError(t, err)
	corruptedData := enc.EncodeAll(badObject, nil)
	for i := 4; i < len(corruptedData); i++ {
		corruptedData[i] ^= 0xFF
	}

	// There are 3 different types of errors to consider.
	// To setup envirionment we use implementation details so this test must be updated
	// if any of them are changed.
	{
		// 1. Invalid object in fs tree.
		// 1.1. Invalid compressed data.
		addr := cidtest.ID().EncodeToString() + "." + objecttest.ID().EncodeToString()
		dirName := filepath.Join(bsPath, addr[:2])
		require.NoError(t, os.MkdirAll(dirName, os.ModePerm))
		require.NoError(t, os.WriteFile(filepath.Join(dirName, addr[2:]), corruptedData, os.ModePerm))

		// 1.2. Unreadable file.
		addr = cidtest.ID().EncodeToString() + "." + objecttest.ID().EncodeToString()
		dirName = filepath.Join(bsPath, addr[:2])
		require.NoError(t, os.MkdirAll(dirName, os.ModePerm))

		fname := filepath.Join(dirName, addr[2:])
		require.NoError(t, os.WriteFile(fname, []byte{}, 0))

		// 1.3. Unreadable dir.
		require.NoError(t, os.MkdirAll(filepath.Join(bsPath, "ZZ"), 0))
	}

	sh = newCustomShard(t, dir, true, wcOpts, bsOpts(3))
	require.NoError(t, sh.SetMode(mode.ReadOnly))

	{
		// 2. Invalid object in peapod.
		var prm common.PutPrm
		prm.Address = oid.Address{}
		prm.RawData = corruptedData
		b := peapod.New(filepath.Join(bsPath, "peapod2.db"), 0o600, 10*time.Millisecond)
		require.NoError(t, b.Open(false))
		require.NoError(t, b.Init())
		_, err := b.Put(prm)
		require.NoError(t, err)
		require.NoError(t, b.Close())
	}

	{
		// 3. Invalid object in write-cache. Note that because shard is read-only
		//    the object won't be flushed.
		addr := cidtest.ID().EncodeToString() + "." + objecttest.ID().EncodeToString()
		dir := filepath.Join(wcPath, addr[:1])
		require.NoError(t, os.MkdirAll(dir, os.ModePerm))
		require.NoError(t, os.WriteFile(filepath.Join(dir, addr[1:]), nil, 0))
	}

	out := filepath.Join(t.TempDir(), "out.dump")
	f, err := os.Create(out)
	require.NoError(t, err)
	res, err := sh.Dump(f, true)
	require.NoError(t, f.Close())
	require.NoError(t, err)
	require.Equal(t, objCount, res)
}
