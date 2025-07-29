package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

type unimplementedBlobstor struct{}

func (unimplementedBlobstor) Open(bool) error {
	panic("unimplemented")
}

func (unimplementedBlobstor) Init() error {
	panic("unimplemented")
}

func (unimplementedBlobstor) Close() error {
	panic("unimplemented")
}

func (unimplementedBlobstor) Type() string {
	panic("unimplemented")
}

func (unimplementedBlobstor) Path() string {
	panic("unimplemented")
}

func (unimplementedBlobstor) SetLogger(*zap.Logger) {
	panic("unimplemented")
}

func (unimplementedBlobstor) SetCompressor(*compression.Config) {
	panic("unimplemented")
}

func (unimplementedBlobstor) GetBytes(oid.Address) ([]byte, error) {
	panic("unimplemented")
}

func (unimplementedBlobstor) Get(oid.Address) (*object.Object, error) {
	panic("unimplemented")
}

func (unimplementedBlobstor) GetRange(oid.Address, uint64, uint64) ([]byte, error) {
	panic("unimplemented")
}

func (unimplementedBlobstor) Head(oid.Address) (*object.Object, error) {
	panic("unimplemented")
}

func (unimplementedBlobstor) Exists(oid.Address) (bool, error) {
	panic("unimplemented")
}

func (unimplementedBlobstor) Put(oid.Address, []byte) error {
	panic("unimplemented")
}

func (unimplementedBlobstor) PutBatch(map[oid.Address][]byte) error {
	panic("unimplemented")
}

func (unimplementedBlobstor) Delete(oid.Address) error {
	panic("unimplemented")
}

func (unimplementedBlobstor) Iterate(func(oid.Address, []byte) error, func(oid.Address, error) error) error {
	panic("unimplemented")
}

func (unimplementedBlobstor) IterateAddresses(func(oid.Address) error, bool) error {
	panic("unimplemented")
}
