package shard

import (
	"os"
	"path"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestRefillMetabase(t *testing.T) {
	p := t.Name()

	defer os.RemoveAll(p)

	blobOpts := []blobstor.Option{
		blobstor.WithRootPath(path.Join(p, "blob")),
		blobstor.WithBlobovniczaShallowWidth(1),
		blobstor.WithBlobovniczaShallowDepth(1),
	}

	sh := New(
		WithBlobStorOptions(blobOpts...),
		WithMetaBaseOptions(
			meta.WithPath(path.Join(p, "meta")),
		),
	)

	// open Blobstor
	require.NoError(t, sh.Open())

	// initialize Blobstor
	require.NoError(t, sh.Init())

	const objNum = 5

	type objAddr struct {
		obj  *object.Object
		addr *objectSDK.Address
	}

	mObjs := make(map[string]objAddr)

	for i := uint64(0); i < objNum; i++ {
		rawObj := objecttest.Raw()
		rawObj.SetType(objectSDK.TypeRegular)

		obj := object.NewFromSDK(rawObj.Object())

		addr := obj.Address()

		mObjs[addr.String()] = objAddr{
			obj:  obj,
			addr: addr,
		}
	}

	tombObjRaw := object.NewRawFrom(objecttest.Raw())
	tombObjRaw.SetType(objectSDK.TypeTombstone)

	tombstone := objecttest.Tombstone()

	tombData, err := tombstone.Marshal()
	require.NoError(t, err)

	tombObjRaw.SetPayload(tombData)

	tombObj := tombObjRaw.Object()

	tombMembers := make([]*objectSDK.Address, 0, len(tombstone.Members()))

	for _, member := range tombstone.Members() {
		a := objectSDK.NewAddress()
		a.SetObjectID(member)
		a.SetContainerID(tombObj.ContainerID())

		tombMembers = append(tombMembers, a)
	}

	var putPrm PutPrm

	for _, v := range mObjs {
		_, err := sh.Put(putPrm.WithObject(v.obj))
		require.NoError(t, err)
	}

	_, err = sh.Put(putPrm.WithObject(tombObj))
	require.NoError(t, err)

	_, err = sh.Inhume(new(InhumePrm).WithTarget(tombObj.Address(), tombMembers...))
	require.NoError(t, err)

	var headPrm HeadPrm

	checkObj := func(addr *objectSDK.Address, expObj *object.Object) {
		res, err := sh.Head(headPrm.WithAddress(addr))

		if expObj == nil {
			require.ErrorIs(t, err, object.ErrNotFound)
			return
		}

		require.NoError(t, err)
		require.Equal(t, object.NewRawFromObject(expObj).CutPayload().Object(), res.Object())
	}

	checkAllObjs := func(exists bool) {
		for _, v := range mObjs {
			if exists {
				checkObj(v.addr, v.obj)
			} else {
				checkObj(v.addr, nil)
			}
		}
	}

	checkTombMembers := func(exists bool) {
		for _, member := range tombMembers {
			_, err := sh.Head(headPrm.WithAddress(member))

			if exists {
				require.ErrorIs(t, err, object.ErrAlreadyRemoved)
			} else {
				require.ErrorIs(t, err, object.ErrNotFound)
			}
		}
	}

	checkAllObjs(true)
	checkObj(tombObj.Address(), tombObj)
	checkTombMembers(true)

	err = sh.Close()
	require.NoError(t, err)

	sh = New(
		WithBlobStorOptions(blobOpts...),
		WithMetaBaseOptions(
			meta.WithPath(path.Join(p, "meta_restored")),
		),
	)

	// open Blobstor
	require.NoError(t, sh.Open())

	// initialize Blobstor
	require.NoError(t, sh.Init())

	defer sh.Close()

	checkAllObjs(false)
	checkObj(tombObj.Address(), nil)
	checkTombMembers(false)

	err = sh.refillMetabase()
	require.NoError(t, err)

	checkAllObjs(true)
	checkObj(tombObj.Address(), tombObj)
	checkTombMembers(true)
}
