package shard

import (
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type epochState struct{}

func (s epochState) CurrentEpoch() uint64 {
	return 0
}

type objAddr struct {
	obj  *objectSDK.Object
	addr oid.Address
}

func TestShardOpen(t *testing.T) {
	dir := t.TempDir()
	metaPath := filepath.Join(dir, "meta")

	newShard := func() *Shard {
		return New(
			WithLogger(zaptest.NewLogger(t)),
			WithBlobstor(fstree.New(
				fstree.WithPath(filepath.Join(dir, "fstree")),
				fstree.WithDepth(1)),
			),
			WithMetaBaseOptions(meta.WithPath(metaPath), meta.WithEpochState(epochState{})),
			WithWriteCache(true),
			WithWriteCacheOptions(
				writecache.WithPath(filepath.Join(dir, "wc"))))
	}

	sh := newShard()
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())
	require.Equal(t, mode.ReadWrite, sh.GetMode())
	require.NoError(t, sh.Close())

	// Metabase can be opened in read-only => start in ReadOnly mode.
	require.NoError(t, os.Chmod(metaPath, 0444))
	sh = newShard()
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())
	require.Equal(t, mode.ReadOnly, sh.GetMode())
	require.Error(t, sh.SetMode(mode.ReadWrite))
	require.Equal(t, mode.ReadOnly, sh.GetMode())
	require.NoError(t, sh.Close())

	// Metabase is corrupted => start in DegradedReadOnly mode.
	require.NoError(t, os.Chmod(metaPath, 0000))
	sh = newShard()
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())
	require.Equal(t, mode.DegradedReadOnly, sh.GetMode())
	require.NoError(t, sh.Close())
}

func TestResyncMetabaseCorrupted(t *testing.T) {
	dir := t.TempDir()

	fsTree := fstree.New(
		fstree.WithPath(filepath.Join(dir, "fstree")),
		fstree.WithDepth(1))

	sh := New(
		WithBlobstor(fsTree),
		WithMetaBaseOptions(meta.WithPath(filepath.Join(dir, "meta")), meta.WithEpochState(epochState{})))
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	obj := objecttest.Object()
	obj.SetType(objectSDK.TypeRegular)
	obj.SetPayload([]byte{0, 1, 2, 3, 4, 5})

	err := sh.Put(&obj, nil)
	require.NoError(t, err)
	require.NoError(t, sh.Close())

	addr := object.AddressOf(&obj)
	// https://github.com/nspcc-dev/neofs-node/issues/2563
	err = fsTree.Delete(addr)
	require.NoError(t, err)
	err = fsTree.Put(addr, []byte("not an object"))
	require.NoError(t, err)

	sh = New(
		WithBlobstor(fsTree),
		WithMetaBaseOptions(meta.WithPath(filepath.Join(dir, "meta_new")), meta.WithEpochState(epochState{})),
		WithResyncMetabase(true))
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	_, err = sh.Get(addr, false)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	require.NoError(t, sh.Close())
}

func TestResyncMetabase(t *testing.T) {
	p := t.Name()

	defer os.RemoveAll(p)

	shID := ID("test")
	sh := New(
		WithBlobstor(fstree.New(
			fstree.WithPath(filepath.Join(p, "fstree")),
			fstree.WithDepth(1)),
		),
		WithID(&shID),
		WithLogger(zaptest.NewLogger(t)),
		WithMetaBaseOptions(
			meta.WithPath(filepath.Join(p, "meta")),
			meta.WithEpochState(epochState{}),
		),
		WithWriteCache(true),
		WithWriteCacheOptions(
			writecache.WithPath(filepath.Join(p, "wc")),
			writecache.WithLogger(zaptest.NewLogger(t)),
		),
	)

	require.NoError(t, sh.UpdateID())

	// open Blobstor
	require.NoError(t, sh.Open())

	// initialize Blobstor
	require.NoError(t, sh.Init())

	const objNum = 10
	oldVersion := version.New(2, 17)

	mObjs := make(map[oid.Address]objAddr)
	locked := make([]oid.ID, 1, 2)
	locked[0] = oidtest.ID()
	cnrLocked := cidtest.ID()
	for i := range uint64(objNum) {
		obj := objecttest.Object()
		obj.SetType(objectSDK.TypeRegular)

		if i < objNum/2 {
			payload := make([]byte, 1024)
			_, _ = rand.Read(payload)

			obj.SetPayload(payload)
		}

		if len(locked) < 2 {
			obj.SetContainerID(cnrLocked)
			id := obj.GetID()
			locked = append(locked, id)
		}

		addr := object.AddressOf(&obj)

		mObjs[addr] = objAddr{
			obj:  &obj,
			addr: addr,
		}
	}

	tombObj := objecttest.Object()
	tombObj.SetType(objectSDK.TypeTombstone)
	tombObj.SetVersion(&oldVersion)

	tombstone := objecttest.Tombstone()

	tombData := tombstone.Marshal()

	tombObj.SetPayload(tombData)

	tombMembers := make([]oid.Address, 0, len(tombstone.Members()))

	members := tombstone.Members()
	for i := range tombstone.Members() {
		var a oid.Address
		a.SetObject(members[i])
		cnr := tombObj.GetContainerID()
		a.SetContainer(cnr)

		tombMembers = append(tombMembers, a)
	}

	for _, v := range mObjs {
		err := sh.Put(v.obj, nil)
		require.NoError(t, err)
	}

	err := sh.Put(&tombObj, nil)
	require.NoError(t, err)

	// LOCK object handling
	var lock objectSDK.Lock
	lock.WriteMembers(locked)

	lockObj := objecttest.Object()
	lockObj.SetVersion(&oldVersion)
	lockObj.SetContainerID(cnrLocked)
	lockObj.WriteLock(lock)

	err = sh.Put(&lockObj, nil)
	require.NoError(t, err)

	lockID := lockObj.GetID()
	require.NoError(t, sh.Lock(cnrLocked, lockID, locked))

	err = sh.Inhume(object.AddressOf(&tombObj), 0, tombMembers...)
	require.NoError(t, err)

	checkObj := func(addr oid.Address, expObj *objectSDK.Object) {
		res, err := sh.Head(addr, false)

		if expObj == nil {
			require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
			return
		}

		require.NoError(t, err)
		require.Equal(t, expObj.CutPayload(), res)
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
			_, err := sh.Head(member, false)

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

			err := sh.MarkGarbage(false, addr)
			require.ErrorAs(t, err, new(apistatus.ObjectLocked),
				"object %s should be locked", locked[i])
		}
	}

	checkAllObjs(true)
	checkObj(object.AddressOf(&tombObj), &tombObj)
	checkTombMembers(true)
	checkLocked(t, cnrLocked, locked)

	c, err := sh.metaBase.ObjectCounters()
	require.NoError(t, err)

	phyBefore := c.Phy()
	logicalBefore := c.Logic()

	err = sh.Close()
	require.NoError(t, err)

	sh = New(
		WithBlobstor(fstree.New(
			fstree.WithPath(filepath.Join(p, "fstree")),
			fstree.WithDepth(1)),
		),
		WithID(&shID),
		WithLogger(zaptest.NewLogger(t)),
		WithMetaBaseOptions(
			meta.WithPath(filepath.Join(p, "meta_restored")),
			meta.WithEpochState(epochState{}),
		),
		WithWriteCache(true),
		WithWriteCacheOptions(
			writecache.WithPath(filepath.Join(p, "wc")),
			writecache.WithLogger(zaptest.NewLogger(t)),
		),
	)

	require.NoError(t, sh.UpdateID())

	// open Blobstor
	require.NoError(t, sh.Open())

	// initialize Blobstor
	require.NoError(t, sh.Init())

	defer sh.Close()

	checkAllObjs(false)
	checkObj(object.AddressOf(&tombObj), nil)
	checkTombMembers(false)

	err = sh.resyncMetabase()
	require.NoError(t, err)

	c, err = sh.metaBase.ObjectCounters()
	require.NoError(t, err)

	require.Equal(t, phyBefore, c.Phy())
	require.Equal(t, logicalBefore, c.Logic())

	checkAllObjs(true)
	checkObj(object.AddressOf(&tombObj), &tombObj)
	checkTombMembers(true)
	checkLocked(t, cnrLocked, locked)
}
