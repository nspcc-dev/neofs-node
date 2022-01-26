package blobstor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	"github.com/stretchr/testify/require"
)

func TestIterateObjects(t *testing.T) {
	p := t.Name()

	const smalSz = 50

	// create BlobStor instance
	blobStor := New(
		WithCompressObjects(true),
		WithRootPath(p),
		WithSmallSizeLimit(smalSz),
		WithBlobovniczaShallowWidth(1),
		WithBlobovniczaShallowDepth(1),
	)

	defer os.RemoveAll(p)

	// open Blobstor
	require.NoError(t, blobStor.Open())

	// initialize Blobstor
	require.NoError(t, blobStor.Init())

	defer blobStor.Close()

	const objNum = 5

	type addrData struct {
		big  bool
		addr *addressSDK.Address
		data []byte
	}

	mObjs := make(map[string]addrData)

	for i := uint64(0); i < objNum; i++ {
		sz := smalSz

		big := i < objNum/2
		if big {
			sz++
		}

		data := make([]byte, sz)
		binary.BigEndian.PutUint64(data, i)

		addr := objecttest.Address()

		mObjs[string(data)] = addrData{
			big:  big,
			addr: addr,
			data: data,
		}
	}

	for _, v := range mObjs {
		_, err := blobStor.PutRaw(v.addr, v.data, true)
		require.NoError(t, err)
	}

	err := IterateBinaryObjects(blobStor, func(data []byte, blzID *blobovnicza.ID) error {
		v, ok := mObjs[string(data)]
		require.True(t, ok)

		require.Equal(t, v.data, data)

		if v.big {
			require.Nil(t, blzID)
		} else {
			require.NotNil(t, blzID)
		}

		delete(mObjs, string(data))

		return nil
	})
	require.NoError(t, err)
	require.Empty(t, mObjs)
}

func TestIterate_IgnoreErrors(t *testing.T) {
	dir := t.TempDir()

	const (
		smallSize = 512
		objCount  = 5
	)
	bsOpts := []Option{
		WithCompressObjects(true),
		WithRootPath(dir),
		WithSmallSizeLimit(smallSize * 2), // + header
		WithBlobovniczaOpenedCacheSize(1),
		WithBlobovniczaShallowWidth(1),
		WithBlobovniczaShallowDepth(1)}
	bs := New(bsOpts...)
	require.NoError(t, bs.Open())
	require.NoError(t, bs.Init())

	addrs := make([]*addressSDK.Address, objCount)
	for i := range addrs {
		addrs[i] = objecttest.Address()
		obj := object.NewRaw()
		obj.SetContainerID(addrs[i].ContainerID())
		obj.SetID(addrs[i].ObjectID())
		obj.SetPayload(make([]byte, smallSize<<(i%2)))

		objData, err := obj.Marshal()
		require.NoError(t, err)

		_, err = bs.PutRaw(addrs[i], objData, true)
		require.NoError(t, err)
	}

	// Construct corrupted compressed object.
	buf := bytes.NewBuffer(nil)
	badObject := make([]byte, smallSize/2+1)
	enc, err := zstd.NewWriter(buf)
	require.NoError(t, err)
	rawData := enc.EncodeAll(badObject, nil)
	for i := 4; /* magic size */ i < len(rawData); i += 2 {
		rawData[i] ^= 0xFF
	}
	// Will be put uncompressed but fetched as compressed because of magic.
	_, err = bs.PutRaw(objecttest.Address(), rawData, false)
	require.NoError(t, err)
	require.NoError(t, bs.fsTree.Put(objecttest.Address(), rawData))

	require.NoError(t, bs.Close())

	// Increase width to have blobovnicza which is definitely empty.
	b := New(append(bsOpts, WithBlobovniczaShallowWidth(2))...)
	require.NoError(t, b.Open())
	require.NoError(t, b.Init())

	var p string
	for i := 0; i < 2; i++ {
		bp := filepath.Join(bs.blzRootPath, "1", strconv.FormatUint(uint64(i), 10))
		if _, ok := bs.blobovniczas.opened.Get(bp); !ok {
			p = bp
			break
		}
	}
	require.NotEqual(t, "", p, "expected to not have at least 1 blobovnicza in cache")
	require.NoError(t, os.Chmod(p, 0))

	var prm IteratePrm
	prm.SetIterationHandler(func(e IterationElement) error {
		return nil
	})
	_, err = bs.Iterate(prm)
	require.Error(t, err)

	prm.IgnoreErrors()

	t.Run("skip invalid objects", func(t *testing.T) {
		actual := make([]*addressSDK.Address, 0, len(addrs))
		prm.SetIterationHandler(func(e IterationElement) error {
			obj := object.New()
			err := obj.Unmarshal(e.data)
			if err != nil {
				return err
			}

			addr := addressSDK.NewAddress()
			addr.SetContainerID(obj.ContainerID())
			addr.SetObjectID(obj.ID())
			actual = append(actual, addr)
			return nil
		})

		_, err := bs.Iterate(prm)
		require.NoError(t, err)
		require.ElementsMatch(t, addrs, actual)
	})
	t.Run("return errors from handler", func(t *testing.T) {
		n := 0
		expectedErr := errors.New("expected error")
		prm.SetIterationHandler(func(e IterationElement) error {
			if n++; n == objCount/2 {
				return expectedErr
			}
			return nil
		})
		_, err := bs.Iterate(prm)
		require.True(t, errors.Is(err, expectedErr), "got: %v")
	})
}
