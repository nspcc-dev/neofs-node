package common

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"

// Storage represents key-value object storage.
// It is used as a building block for a blobstor of a shard.
type Storage interface {
	Open(readOnly bool) error
	Init() error
	Close() error

	Type() string
	SetCompressor(cc *compression.Config)

	Get(GetPrm) (GetRes, error)
	GetRange(GetRangePrm) (GetRangeRes, error)
	Exists(ExistsPrm) (ExistsRes, error)
	Put(PutPrm) (PutRes, error)
	Delete(DeletePrm) (DeleteRes, error)
	Iterate(IteratePrm) (IterateRes, error)
}
