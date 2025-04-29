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
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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

func (x *getBytesOnlySubStorage) Get(_ oid.Address) (*objectSDK.Object, error) {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) GetRange(_ oid.Address, _ uint64, _ uint64) ([]byte, error) {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) Exists(_ oid.Address) (bool, error) {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) Put(oid.Address, []byte) error {
	panic("must not be called")
}
func (x *getBytesOnlySubStorage) PutBatch(map[oid.Address][]byte) error {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) Delete(_ oid.Address) error {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) Iterate(_ func(oid.Address, []byte) error, _ func(oid.Address, error) error) error {
	panic("must not be called")
}

func (x *getBytesOnlySubStorage) IterateAddresses(_ func(oid.Address) error, _ bool) error {
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
	newBlobStorWithStorages := func(ss common.Storage) *BlobStor {
		return &BlobStor{cfg: cfg{log: zap.NewNop(), storage: SubStorage{Storage: ss}}}
	}

	obj := objecttest.Object()
	addr := object.AddressOf(&obj)
	objBin := obj.Marshal()

	bs := newBlobStorWithStorages(&getBytesOnlySubStorage{
		m: map[oid.Address][]byte{addr: objBin},
	})

	b, err := bs.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)

	t.Run("not found", func(t *testing.T) {
		bs := newBlobStorWithStorages(new(getBytesOnlySubStorage))

		_, err := bs.GetBytes(oidtest.Address())
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		require.ErrorIs(t, err, logicerr.Error)
	})

	t.Run("other storage failure", func(t *testing.T) {
		errStorage := errors.New("any storage error")
		bs := newBlobStorWithStorages(&getBytesOnlySubStorage{
			e: errStorage,
		})

		_, err := bs.GetBytes(oidtest.Address())
		require.ErrorIs(t, err, errStorage)
	})
}
