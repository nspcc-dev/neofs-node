package shard

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestRefillMetabaseCorrupted(t *testing.T) {
	dir := t.TempDir()

	blobOpts := []blobstor.Option{
		blobstor.WithRootPath(filepath.Join(dir, "blob")),
		blobstor.WithShallowDepth(1),
		blobstor.WithSmallSizeLimit(1),
		blobstor.WithBlobovniczaShallowWidth(1),
		blobstor.WithBlobovniczaShallowDepth(1)}

	sh := New(
		WithBlobStorOptions(blobOpts...),
		WithPiloramaOptions(pilorama.WithPath(filepath.Join(dir, "pilorama"))),
		WithMetaBaseOptions(meta.WithPath(filepath.Join(dir, "meta"))))
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	obj := objecttest.Object()
	obj.SetType(objectSDK.TypeRegular)
	obj.SetPayload([]byte{0, 1, 2, 3, 4, 5})

	var putPrm PutPrm
	putPrm.WithObject(obj)
	_, err := sh.Put(putPrm)
	require.NoError(t, err)
	require.NoError(t, sh.Close())

	addr := object.AddressOf(obj)
	fs := fstree.FSTree{
		DirNameLen: 2,
		Depth:      1,
		Info:       sh.blobStor.DumpInfo(),
	}
	require.NoError(t, fs.Put(addr, []byte("not an object")))

	sh = New(
		WithBlobStorOptions(blobOpts...),
		WithPiloramaOptions(pilorama.WithPath(filepath.Join(dir, "pilorama"))),
		WithMetaBaseOptions(meta.WithPath(filepath.Join(dir, "meta_new"))),
		WithRefillMetabase(true))
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	var getPrm GetPrm
	getPrm.WithAddress(addr)
	_, err = sh.Get(getPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	require.NoError(t, sh.Close())
}

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
		WithPiloramaOptions(
			pilorama.WithPath(filepath.Join(p, "pilorama"))),
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
	locked := make([]oid.ID, 1, 2)
	locked[0] = oidtest.ID()
	cnrLocked := cidtest.ID()
	for i := uint64(0); i < objNum; i++ {
		obj := objecttest.Object()
		obj.SetType(objectSDK.TypeRegular)

		if len(locked) < 2 {
			obj.SetContainerID(cnrLocked)
			id, _ := obj.ID()
			locked = append(locked, id)
		}

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

	// LOCK object handling
	var lock objectSDK.Lock
	lock.WriteMembers(locked)

	lockObj := objecttest.Object()
	lockObj.SetContainerID(cnrLocked)
	objectSDK.WriteLock(lockObj, lock)

	putPrm.WithObject(lockObj)
	_, err = sh.Put(putPrm)
	require.NoError(t, err)

	lockID, _ := lockObj.ID()
	require.NoError(t, sh.Lock(cnrLocked, lockID, locked))

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

	checkLocked := func(t *testing.T, cnr cid.ID, locked []oid.ID) {
		var addr oid.Address
		addr.SetContainer(cnr)

		for i := range locked {
			addr.SetObject(locked[i])

			var prm InhumePrm
			prm.MarkAsGarbage(addr)

			_, err := sh.Inhume(prm)
			require.ErrorAs(t, err, new(apistatus.ObjectLocked),
				"object %s should be locked", locked[i])
		}
	}

	checkAllObjs(true)
	checkObj(object.AddressOf(tombObj), tombObj)
	checkTombMembers(true)
	checkLocked(t, cnrLocked, locked)

	err = sh.Close()
	require.NoError(t, err)

	sh = New(
		WithBlobStorOptions(blobOpts...),
		WithMetaBaseOptions(
			meta.WithPath(filepath.Join(p, "meta_restored")),
		),
		WithPiloramaOptions(
			pilorama.WithPath(filepath.Join(p, "pilorama_another"))),
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
	checkLocked(t, cnrLocked, locked)
}
