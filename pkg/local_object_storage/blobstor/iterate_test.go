package blobstor

import (
	"encoding/binary"
	"os"
	"testing"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestIterateObjects(t *testing.T) {
	p := t.Name()

	const smalSz = 50

	// create BlobStor instance
	blobStor := New(
		WithStorages(defaultStorages(p, smalSz)),
		WithCompressObjects(true),
	)

	defer os.RemoveAll(p)

	// open Blobstor
	require.NoError(t, blobStor.Open(false))

	// initialize Blobstor
	require.NoError(t, blobStor.Init())

	defer blobStor.Close()

	const objNum = 5

	type addrData struct {
		big  bool
		addr oid.Address
		data []byte
	}

	mObjs := make(map[string]addrData)

	for i := range uint64(objNum) {
		sz := smalSz

		big := i < objNum/2
		if big {
			sz++
		}

		data := make([]byte, sz)
		binary.BigEndian.PutUint64(data, i)

		addr := oidtest.Address()

		mObjs[string(data)] = addrData{
			big:  big,
			addr: addr,
			data: data,
		}
	}

	for _, v := range mObjs {
		_, err := blobStor.Put(v.addr, nil, v.data)
		require.NoError(t, err)
	}

	err := blobStor.IterateBinaryObjects(func(addr oid.Address, data []byte, descriptor []byte) error {
		v, ok := mObjs[string(data)]
		require.True(t, ok)

		require.Equal(t, v.data, data)

		if v.big {
			require.True(t, descriptor != nil && len(descriptor) == 0)
		} else {
			require.NotEmpty(t, descriptor)
		}

		delete(mObjs, string(data))

		return nil
	})
	require.NoError(t, err)
	require.Empty(t, mObjs)
}

func TestIterate_IgnoreErrors(t *testing.T) {
	t.Skip()
	//dir := t.TempDir()
	//
	//const (
	//	smallSize = 512
	//	objCount  = 5
	//)
	//bsOpts := []Option{
	//	WithCompressObjects(true),
	//	WithRootPath(dir),
	//	WithSmallSizeLimit(smallSize * 2), // + header
	//	WithBlobovniczaOpenedCacheSize(1),
	//	WithBlobovniczaShallowWidth(1),
	//	WithBlobovniczaShallowDepth(1)}
	//bs := New(bsOpts...)
	//require.NoError(t, bs.Open(false))
	//require.NoError(t, bs.Init())
	//
	//addrs := make([]oid.Address, objCount)
	//for i := range addrs {
	//	addrs[i] = oidtest.Address()
	//
	//	obj := object.New()
	//	obj.SetContainerID(addrs[i].Container())
	//	obj.SetID(addrs[i].Object())
	//	obj.SetPayload(make([]byte, smallSize<<(i%2)))
	//
	//	objData, err := obj.Marshal()
	//	require.NoError(t, err)
	//
	//	_, err = bs.PutRaw(addrs[i], objData, true)
	//	require.NoError(t, err)
	//}
	//
	//// Construct corrupted compressed object.
	//buf := bytes.NewBuffer(nil)
	//badObject := make([]byte, smallSize/2+1)
	//enc, err := zstd.NewWriter(buf)
	//require.NoError(t, err)
	//rawData := enc.EncodeAll(badObject, nil)
	//for i := 4; /* magic size */ i < len(rawData); i += 2 {
	//	rawData[i] ^= 0xFF
	//}
	//// Will be put uncompressed but fetched as compressed because of magic.
	//_, err = bs.PutRaw(oidtest.Address(), rawData, false)
	//require.NoError(t, err)
	//require.NoError(t, bs.fsTree.Put(oidtest.Address(), rawData))
	//
	//require.NoError(t, bs.Close())
	//
	//// Increase width to have blobovnicza which is definitely empty.
	//b := New(append(bsOpts, WithBlobovniczaShallowWidth(2))...)
	//require.NoError(t, b.Open(false))
	//require.NoError(t, b.Init())
	//
	//var p string
	//for i := range 2 {
	//	bp := filepath.Join(bs.rootPath, "1", strconv.FormatUint(uint64(i), 10))
	//	if _, ok := bs.blobovniczas.opened.Get(bp); !ok {
	//		p = bp
	//		break
	//	}
	//}
	//require.NotEqual(t, "", p, "expected to not have at least 1 blobovnicza in cache")
	//require.NoError(t, os.Chmod(p, 0))
	//
	//require.NoError(t, b.Close())
	//require.NoError(t, bs.Open(false))
	//require.NoError(t, bs.Init())
	//
	//var prm IteratePrm
	//prm.SetIterationHandler(func(e IterationElement) error {
	//	return nil
	//})
	//_, err = bs.Iterate(prm)
	//require.Error(t, err)
	//
	//prm.IgnoreErrors()
	//
	//t.Run("skip invalid objects", func(t *testing.T) {
	//	actual := make([]oid.Address, 0, len(addrs))
	//	prm.SetIterationHandler(func(e IterationElement) error {
	//		obj := object.New()
	//		err := obj.Unmarshal(e.data)
	//		if err != nil {
	//			return err
	//		}
	//
	//		var addr oid.Address
	//		cnr, _ := obj.ContainerID()
	//		addr.SetContainer(cnr)
	//		id, _ := obj.ID()
	//		addr.SetObject(id)
	//		actual = append(actual, addr)
	//		return nil
	//	})
	//
	//	_, err := bs.Iterate(prm)
	//	require.NoError(t, err)
	//	require.ElementsMatch(t, addrs, actual)
	//})
	//t.Run("return errors from handler", func(t *testing.T) {
	//	n := 0
	//	expectedErr := errors.New("expected error")
	//	prm.SetIterationHandler(func(e IterationElement) error {
	//		if n++; n == objCount/2 {
	//			return expectedErr
	//		}
	//		return nil
	//	})
	//	_, err := bs.Iterate(prm)
	//	require.ErrorIs(t, err, expectedErr)
	//})
}
