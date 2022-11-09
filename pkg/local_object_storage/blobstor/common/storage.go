package common

import "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"

// Storage represents key-value object storage.
// It is used as a building block for a blobstor of a shard.
type Storage interface {
	Open(readOnly bool) error
	Init() error
	Close() error

	Type() string
	Path() string
	SetCompressor(cc *compression.Config)
	// SetReportErrorFunc allows to provide a function to be called on disk errors.
	// This function MUST be called before Open.
	SetReportErrorFunc(f func(string, error))

	Get(GetPrm) (GetRes, error)
	GetRange(GetRangePrm) (GetRangeRes, error)
	Exists(ExistsPrm) (ExistsRes, error)
	Put(PutPrm) (PutRes, error)
	Delete(DeletePrm) (DeleteRes, error)
	Iterate(IteratePrm) (IterateRes, error)
}
