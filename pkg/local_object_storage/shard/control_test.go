package shard

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestRefillMetabase(t *testing.T) {
	p := t.Name()

	defer os.RemoveAll(p)

	blobOpts := []blobstor.Option{
		blobstor.WithRootPath(filepath.Join(p, "blob")),
		blobstor.WithBlobovniczaShallowWidth(1),
		blobstor.WithBlobovniczaShallowDepth(1),
	}

	sh := New(
		WithBlobStorOptions(blobOpts...),
		WithMetaBaseOptions(
			meta.WithPath(filepath.Join(p, "meta")),
		),
	)

	// open Blobstor
	require.NoError(t, sh.Open())

	// initialize Blobstor
	require.NoError(t, sh.Init())

	const objNum = 5

	type objAddr struct {
		obj  *objectSDK.Object
		addr oid.Address
	}

	mObjs := make(map[string]objAddr)

	for i := uint64(0); i < objNum; i++ {
		obj := objecttest.Object()
		obj.SetType(objectSDK.TypeRegular)

		addr := object.AddressOf(obj)

		mObjs[addr.EncodeToString()] = objAddr{
			obj:  obj,
			addr: addr,
		}
	}

	tombObj := objecttest.Object()
	tombObj.SetType(objectSDK.TypeTombstone)

	tombstone := objecttest.Tombstone()

	tombData, err := tombstone.Marshal()
	require.NoError(t, err)

	tombObj.SetPayload(tombData)

	tombMembers := make([]oid.Address, 0, len(tombstone.Members()))

	members := tombstone.Members()
	for i := range tombstone.Members() {
		var a oid.Address
		a.SetObject(members[i])
		cnr, _ := tombObj.ContainerID()
		a.SetContainer(cnr)

		tombMembers = append(tombMembers, a)
	}

	var putPrm PutPrm

	for _, v := range mObjs {
		putPrm.WithObject(v.obj)

		_, err := sh.Put(putPrm)
		require.NoError(t, err)
	}

	putPrm.WithObject(tombObj)

	_, err = sh.Put(putPrm)
	require.NoError(t, err)

	var inhumePrm InhumePrm
	inhumePrm.WithTarget(object.AddressOf(tombObj), tombMembers...)

	_, err = sh.Inhume(inhumePrm)
	require.NoError(t, err)

	var headPrm HeadPrm

	checkObj := func(addr oid.Address, expObj *objectSDK.Object) {
		headPrm.WithAddress(addr)

		res, err := sh.Head(headPrm)

		if expObj == nil {
			require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
			return
		}

		require.NoError(t, err)
		require.Equal(t, expObj.CutPayload(), res.Object())
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
			headPrm.WithAddress(member)

			_, err := sh.Head(headPrm)

			if exists {
				require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
			} else {
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
			}
		}
	}

	checkAllObjs(true)
	checkObj(object.AddressOf(tombObj), tombObj)
	checkTombMembers(true)

	err = sh.Close()
	require.NoError(t, err)

	sh = New(
		WithBlobStorOptions(blobOpts...),
		WithMetaBaseOptions(
			meta.WithPath(filepath.Join(p, "meta_restored")),
		),
	)

	// open Blobstor
	require.NoError(t, sh.Open())

	// initialize Blobstor
	require.NoError(t, sh.Init())

	defer sh.Close()

	checkAllObjs(false)
	checkObj(object.AddressOf(tombObj), nil)
	checkTombMembers(false)

	err = sh.refillMetabase()
	require.NoError(t, err)

	checkAllObjs(true)
	checkObj(object.AddressOf(tombObj), tombObj)
	checkTombMembers(true)
}
