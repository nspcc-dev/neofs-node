package blobstor_test

import (
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

type mockWriter struct {
	common.Storage
	full bool
}

func (x *mockWriter) Type() string {
	if x.full {
		return "full"
	}
	return "free"
}

func (x *mockWriter) Put(oid.Address, []byte) error {
	if x.full {
		return common.ErrNoSpace
	}
	return nil
}

func (x *mockWriter) PutBatch(map[oid.Address][]byte) error {
	if x.full {
		return common.ErrNoSpace
	}
	return nil
}

func (x *mockWriter) SetCompressor(*compression.Config) {}

func TestBlobStor_Put_Overflow(t *testing.T) {
	sub1 := &mockWriter{full: false}
	bs := blobstor.New(blobstor.WithStorages(
		blobstor.SubStorage{
			Storage: sub1,
		},
	))

	addr := oidtest.Address()

	obj := objecttest.Object()
	obj.SetContainerID(addr.Container())
	obj.SetID(addr.Object())

	err := bs.Put(addr, &obj, nil)
	require.NoError(t, err)
	err = bs.PutBatch([]blobstor.PutBatchPrm{{Addr: addr, Obj: &obj}})
	require.NoError(t, err)

	sub1.full = true

	err = bs.Put(addr, &obj, nil)
	require.ErrorIs(t, err, common.ErrNoSpace)
	err = bs.PutBatch([]blobstor.PutBatchPrm{{Addr: addr, Obj: &obj}})
	require.ErrorIs(t, err, common.ErrNoSpace)
}

func TestBlobStor_PutBatch(t *testing.T) {
	dir := t.TempDir()
	fs := fstree.New(fstree.WithPath(filepath.Join(dir, "fstree")))
	bs := blobstor.New(blobstor.WithStorages(blobstor.SubStorage{Storage: fs}))
	require.NoError(t, bs.Open(false))
	require.NoError(t, bs.Init())

	const objCount = 5
	objs := make([]blobstor.PutBatchPrm, objCount)
	for i := range objCount {
		obj := objecttest.Object()
		addr := oidtest.Address()
		obj.SetContainerID(addr.Container())
		obj.SetID(addr.Object())

		objs[i] = blobstor.PutBatchPrm{Addr: addr, Obj: &obj}
	}

	err := bs.PutBatch(objs)
	require.NoError(t, err)

	for i := range objCount {
		obj, err := bs.Get(objs[i].Addr)
		require.NoError(t, err)
		require.Equal(t, objs[i].Obj, obj)
	}
}
