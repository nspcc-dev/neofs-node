package shard_test

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
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
		bsSmallObjectSize = 10 * 1024     // 10 KiB, goes to blobovnicza DB
		bsBigObjectSize   = 1024*1024 + 1 // > 1 MiB, goes to blobovnicza FSTree
	)

	var sh *shard.Shard
	if !hasWriteCache {
		sh = newShard(t, false)
	} else {
		sh = newCustomShard(t, t.TempDir(), true,
			[]writecache.Option{
				writecache.WithSmallObjectSize(wcSmallObjectSize),
				writecache.WithMaxObjectSize(wcBigObjectSize),
			},
			nil)
	}
	defer releaseShard(sh, t)

	out := filepath.Join(t.TempDir(), "dump")
	var prm shard.DumpPrm
	prm.WithPath(out)

	t.Run("must be read-only", func(t *testing.T) {
		_, err := sh.Dump(prm)
		require.ErrorIs(t, err, shard.ErrMustBeReadOnly)
	})

	require.NoError(t, sh.SetMode(shard.ModeReadOnly))
	outEmpty := out + ".empty"
	var dumpPrm shard.DumpPrm
	dumpPrm.WithPath(outEmpty)

	res, err := sh.Dump(dumpPrm)
	require.NoError(t, err)
	require.Equal(t, 0, res.Count())
	require.NoError(t, sh.SetMode(shard.ModeReadWrite))

	// Approximate object header size.
	const headerSize = 400

	objects := make([]*objectSDK.Object, objCount)
	for i := 0; i < objCount; i++ {
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
		rand.Read(data)
		obj := generateObjectWithPayload(cnr, data)
		objects[i] = obj

		var prm shard.PutPrm
		prm.WithObject(objects[i])
		_, err := sh.Put(prm)
		require.NoError(t, err)
	}

	require.NoError(t, sh.SetMode(shard.ModeReadOnly))

	t.Run("invalid path", func(t *testing.T) {
		var dumpPrm shard.DumpPrm
		dumpPrm.WithPath("\x00")

		_, err := sh.Dump(dumpPrm)
		require.Error(t, err)
	})

	res, err = sh.Dump(prm)
	require.NoError(t, err)
	require.Equal(t, objCount, res.Count())

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
					defer releaseShard(sh, t)

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
			require.NoError(t, sh.SetMode(shard.ModeReadOnly))

			_, err := sh.Restore(prm)
			require.ErrorIs(t, err, shard.ErrReadOnlyMode)
		})

		require.NoError(t, sh.SetMode(shard.ModeReadWrite))

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
	for i := 0; i < objCount; i++ {
		cnr := cidtest.ID()
		obj := generateObjectWithCID(t, cnr)
		objects[i] = obj

		var prm shard.PutPrm
		prm.WithObject(objects[i])
		_, err := sh1.Put(prm)
		require.NoError(t, err)
	}

	require.NoError(t, sh1.SetMode(shard.ModeReadOnly))

	r, w := io.Pipe()
	finish := make(chan struct{})

	go func() {
		var dumpPrm shard.DumpPrm
		dumpPrm.WithStream(w)

		res, err := sh1.Dump(dumpPrm)
		require.NoError(t, err)
		require.Equal(t, objCount, res.Count())
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

	var getPrm shard.GetPrm

	for i := range objects {
		getPrm.WithAddress(object.AddressOf(objects[i]))
		res, err := sh.Get(getPrm)
		require.NoError(t, err)
		require.Equal(t, objects[i], res.Object())
	}
}

func TestDumpIgnoreErrors(t *testing.T) {
	const (
		wcSmallObjectSize = 512                    // goes to write-cache memory
		wcBigObjectSize   = wcSmallObjectSize << 1 // goes to write-cache FSTree
		bsSmallObjectSize = wcSmallObjectSize << 2 // goes to blobovnicza DB

		objCount   = 10
		headerSize = 400
	)

	dir := t.TempDir()
	bsPath := filepath.Join(dir, "blob")
	bsOpts := []blobstor.Option{
		blobstor.WithSmallSizeLimit(bsSmallObjectSize),
		blobstor.WithRootPath(bsPath),
		blobstor.WithCompressObjects(true),
		blobstor.WithShallowDepth(1),
		blobstor.WithBlobovniczaShallowDepth(1),
		blobstor.WithBlobovniczaShallowWidth(2),
		blobstor.WithBlobovniczaOpenedCacheSize(1),
	}
	wcPath := filepath.Join(dir, "writecache")
	wcOpts := []writecache.Option{
		writecache.WithPath(wcPath),
		writecache.WithSmallObjectSize(wcSmallObjectSize),
		writecache.WithMaxObjectSize(wcBigObjectSize),
	}
	sh := newCustomShard(t, dir, true, wcOpts, bsOpts)

	objects := make([]*objectSDK.Object, objCount)
	for i := 0; i < objCount; i++ {
		size := (wcSmallObjectSize << (i % 4)) - headerSize
		obj := generateObjectWithPayload(cidtest.ID(), make([]byte, size))
		objects[i] = obj

		var prm shard.PutPrm
		prm.WithObject(objects[i])
		_, err := sh.Put(prm)
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

	bsOpts = append(bsOpts, blobstor.WithBlobovniczaShallowWidth(3))
	sh = newCustomShard(t, dir, true, wcOpts, bsOpts)
	require.NoError(t, sh.SetMode(shard.ModeReadOnly))

	{
		// 2. Invalid object in blobovnicza.
		// 2.1. Invalid blobovnicza.
		bTree := filepath.Join(bsPath, "blobovnicza")
		data := make([]byte, 1024)
		rand.Read(data)
		require.NoError(t, os.WriteFile(filepath.Join(bTree, "0", "2"), data, 0))

		// 2.2. Invalid object in valid blobovnicza.
		prm := new(blobovnicza.PutPrm)
		prm.SetAddress(oid.Address{})
		prm.SetMarshaledObject(corruptedData)
		b := blobovnicza.New(blobovnicza.WithPath(filepath.Join(bTree, "1", "2")))
		require.NoError(t, b.Open())
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
	var dumpPrm shard.DumpPrm
	dumpPrm.WithPath(out)
	dumpPrm.WithIgnoreErrors(true)
	res, err := sh.Dump(dumpPrm)
	require.NoError(t, err)
	require.Equal(t, objCount, res.Count())
}
