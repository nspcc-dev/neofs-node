package shard_test

import (
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestEvacuate(t *testing.T) {
	sh := newShard(t, false)
	defer releaseShard(sh, t)

	out := filepath.Join(t.TempDir(), "dump")
	prm := new(shard.EvacuatePrm).WithPath(out)

	t.Run("must be read-only", func(t *testing.T) {
		_, err := sh.Evacuate(prm)
		require.True(t, errors.Is(err, shard.ErrMustBeReadOnly), "got: %v", err)
	})

	require.NoError(t, sh.SetMode(shard.ModeReadOnly))
	outEmpty := out + ".empty"
	res, err := sh.Evacuate(new(shard.EvacuatePrm).WithPath(outEmpty))
	require.NoError(t, err)
	require.Equal(t, 0, res.Count())
	require.NoError(t, sh.SetMode(shard.ModeReadWrite))

	const objCount = 10
	objects := make([]*object.Object, objCount)
	for i := 0; i < objCount; i++ {
		cid := cidtest.ID()
		obj := generateRawObjectWithCID(t, cid)
		addAttribute(obj, "foo", strconv.FormatUint(rand.Uint64(), 10))
		objects[i] = obj.Object()

		prm := new(shard.PutPrm).WithObject(objects[i])
		_, err := sh.Put(prm)
		require.NoError(t, err)
	}

	require.NoError(t, sh.SetMode(shard.ModeReadOnly))

	t.Run("invalid path", func(t *testing.T) {
		_, err := sh.Evacuate(new(shard.EvacuatePrm).WithPath("\x00"))
		require.Error(t, err)
	})

	res, err = sh.Evacuate(prm)
	require.NoError(t, err)
	require.Equal(t, objCount, res.Count())

	t.Run("restore", func(t *testing.T) {
		sh := newShard(t, false)
		defer releaseShard(sh, t)

		t.Run("empty dump", func(t *testing.T) {
			res, err := sh.Restore(new(shard.RestorePrm).WithPath(outEmpty))
			require.NoError(t, err)
			require.Equal(t, 0, res.Count())
		})

		t.Run("invalid path", func(t *testing.T) {
			_, err := sh.Restore(new(shard.RestorePrm))
			require.True(t, errors.Is(err, os.ErrNotExist), "got: %v", err)
		})

		t.Run("invalid file", func(t *testing.T) {
			t.Run("invalid magic", func(t *testing.T) {
				out := out + ".wrongmagic"
				require.NoError(t, ioutil.WriteFile(out, []byte{0, 0, 0, 0}, os.ModePerm))

				_, err := sh.Restore(new(shard.RestorePrm).WithPath(out))
				require.True(t, errors.Is(err, shard.ErrInvalidMagic), "got: %v", err)
			})

			fileData, err := ioutil.ReadFile(out)
			require.NoError(t, err)

			t.Run("incomplete size", func(t *testing.T) {
				out := out + ".wrongsize"
				fileData := append(fileData, 1)
				require.NoError(t, ioutil.WriteFile(out, fileData, os.ModePerm))

				_, err := sh.Restore(new(shard.RestorePrm).WithPath(out))
				require.True(t, errors.Is(err, io.ErrUnexpectedEOF), "got: %v", err)
			})
			t.Run("incomplete object data", func(t *testing.T) {
				out := out + ".wrongsize"
				fileData := append(fileData, 1, 0, 0, 0)
				require.NoError(t, ioutil.WriteFile(out, fileData, os.ModePerm))

				_, err := sh.Restore(new(shard.RestorePrm).WithPath(out))
				require.True(t, errors.Is(err, io.EOF), "got: %v", err)
			})
			t.Run("invalid object", func(t *testing.T) {
				out := out + ".wrongobj"
				fileData := append(fileData, 1, 0, 0, 0, 0xFF)
				require.NoError(t, ioutil.WriteFile(out, fileData, os.ModePerm))

				_, err := sh.Restore(new(shard.RestorePrm).WithPath(out))
				require.Error(t, err)
			})
		})

		prm := new(shard.RestorePrm).WithPath(out)
		t.Run("must allow write", func(t *testing.T) {
			require.NoError(t, sh.SetMode(shard.ModeReadOnly))

			_, err := sh.Restore(prm)
			require.True(t, errors.Is(err, shard.ErrReadOnlyMode), "got: %v", err)
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
	objects := make([]*object.Object, objCount)
	for i := 0; i < objCount; i++ {
		cid := cidtest.ID()
		obj := generateRawObjectWithCID(t, cid)
		objects[i] = obj.Object()

		prm := new(shard.PutPrm).WithObject(objects[i])
		_, err := sh1.Put(prm)
		require.NoError(t, err)
	}

	require.NoError(t, sh1.SetMode(shard.ModeReadOnly))

	r, w := io.Pipe()
	finish := make(chan struct{})

	go func() {
		res, err := sh1.Evacuate(new(shard.EvacuatePrm).WithStream(w))
		require.NoError(t, err)
		require.Equal(t, objCount, res.Count())
		require.NoError(t, w.Close())
		close(finish)
	}()

	checkRestore(t, sh2, new(shard.RestorePrm).WithStream(r), objects)
	require.Eventually(t, func() bool {
		select {
		case <-finish:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond)
}

func checkRestore(t *testing.T, sh *shard.Shard, prm *shard.RestorePrm, objects []*object.Object) {
	res, err := sh.Restore(prm)
	require.NoError(t, err)
	require.Equal(t, len(objects), res.Count())

	for i := range objects {
		_, err := sh.Get(new(shard.GetPrm).WithAddress(objects[i].Address()))
		require.NoError(t, err)
	}
}
