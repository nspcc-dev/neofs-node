package blobstor

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
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
		addr *object.Address
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
		_, err := blobStor.PutRaw(v.addr, v.data)
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
