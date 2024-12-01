package blobstor

import (
	"bytes"
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type getBytesOnlySubStorage struct {
	e error
	m map[oid.Address][]byte
}

func (x *getBytesOnlySubStorage) Open(readOnly bool) error             { panic("must not be called") }
func (x *getBytesOnlySubStorage) Init() error                          { panic("must not be called") }
func (x *getBytesOnlySubStorage) Close() error                         { panic("must not be called") }
func (x *getBytesOnlySubStorage) Type() string                         { panic("must not be called") }
func (x *getBytesOnlySubStorage) Path() string                         { panic("must not be called") }
func (x *getBytesOnlySubStorage) SetCompressor(cc *compression.Config) { panic("must not be called") }
func (x *getBytesOnlySubStorage) SetLogger(_ *zap.Logger)              { panic("must not be called") }

func (x *getBytesOnlySubStorage) Get(prm common.GetPrm) (common.GetRes, error) {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) GetRange(prm common.GetRangePrm) (common.GetRangeRes, error) {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) Exists(prm common.ExistsPrm) (common.ExistsRes, error) {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) Put(prm common.PutPrm) (common.PutRes, error) {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) Delete(_ oid.Address) error {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) Iterate(prm common.IteratePrm) (common.IterateRes, error) {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) GetBytes(addr oid.Address) ([]byte, error) {
	if x.e != nil {
		return nil, x.e
	}
	val, ok := x.m[addr]
	if !ok {
		return nil, apistatus.ErrObjectNotFound
	}
	return bytes.Clone(val), nil
}

func TestBlobStor_GetBytes(t *testing.T) {
	newBlobStorWithStorages := func(ss ...common.Storage) *BlobStor {
		subs := make([]SubStorage, len(ss))
		for i := range ss {
			subs[i].Storage = ss[i]
		}
		return &BlobStor{cfg: cfg{log: zap.NewNop(), storage: subs}}
	}

	obj := objecttest.Object()
	addr := object.AddressOf(&obj)
	objBin := obj.Marshal()

	bs := newBlobStorWithStorages(new(getBytesOnlySubStorage), &getBytesOnlySubStorage{
		m: map[oid.Address][]byte{addr: objBin},
	})

	b, err := bs.GetBytes(addr, nil)
	require.NoError(t, err)
	require.Equal(t, objBin, b)
	b, err = bs.GetBytes(addr, []byte{})
	require.NoError(t, err)
	require.Equal(t, objBin, b)

	t.Run("not found", func(t *testing.T) {
		bs := newBlobStorWithStorages(new(getBytesOnlySubStorage))

		_, err := bs.GetBytes(oidtest.Address(), nil)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		require.ErrorIs(t, err, logicerr.Error)
		_, err = bs.GetBytes(oidtest.Address(), []byte{})
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		require.NotErrorIs(t, err, logicerr.Error)
		_, err = bs.GetBytes(oidtest.Address(), []byte("any"))
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		require.NotErrorIs(t, err, logicerr.Error)
	})

	t.Run("other storage failure", func(t *testing.T) {
		errStorage := errors.New("any storage error")
		bs := newBlobStorWithStorages(&getBytesOnlySubStorage{
			e: errStorage,
		})

		_, err := bs.GetBytes(oidtest.Address(), nil)
		require.ErrorIs(t, err, errStorage)
		_, err = bs.GetBytes(oidtest.Address(), []byte{})
		require.ErrorIs(t, err, errStorage)
		_, err = bs.GetBytes(oidtest.Address(), []byte("any"))
		require.ErrorIs(t, err, errStorage)
	})
}
