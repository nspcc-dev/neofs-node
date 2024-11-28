package common

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Storage represents key-value object storage.
// It is used as a building block for a blobstor of a shard.
type Storage interface {
	Open(readOnly bool) error
	Init() error
	Close() error

	Type() string
	Path() string
	SetLogger(*zap.Logger)
	SetCompressor(cc *compression.Config)

	// GetBytes reads object by address into memory buffer in a canonical NeoFS
	// binary format. Returns [apistatus.ObjectNotFound] if object is missing.
	GetBytes(oid.Address) ([]byte, error)
	Get(GetPrm) (GetRes, error)
	GetRange(GetRangePrm) (GetRangeRes, error)
	Exists(ExistsPrm) (ExistsRes, error)
	Put(PutPrm) (PutRes, error)
	Delete(DeletePrm) (DeleteRes, error)
	Iterate(IteratePrm) (IterateRes, error)
}

// Copy copies all objects from source Storage into the destination one. If any
// object cannot be stored, Copy immediately fails.
func Copy(dst, src Storage) error {
	err := src.Open(true)
	if err != nil {
		return fmt.Errorf("open source sub-storage: %w", err)
	}

	defer func() { _ = src.Close() }()

	err = src.Init()
	if err != nil {
		return fmt.Errorf("initialize source sub-storage: %w", err)
	}

	err = dst.Open(false)
	if err != nil {
		return fmt.Errorf("open destination sub-storage: %w", err)
	}

	defer func() { _ = dst.Close() }()

	err = dst.Init()
	if err != nil {
		return fmt.Errorf("initialize destination sub-storage: %w", err)
	}

	_, err = src.Iterate(IteratePrm{
		Handler: func(el IterationElement) error {
			exRes, err := dst.Exists(ExistsPrm{
				Address: el.Address,
			})
			if err != nil {
				return fmt.Errorf("check presence of object %s in the destination sub-storage: %w", el.Address, err)
			} else if exRes.Exists {
				return nil
			}

			_, err = dst.Put(PutPrm{
				Address: el.Address,
				RawData: el.ObjectData,
			})
			if err != nil {
				return fmt.Errorf("put object %s into destination sub-storage: %w", el.Address, err)
			}
			return nil
		},
	})
	if err != nil {
		return fmt.Errorf("iterate over source sub-storage: %w", err)
	}

	return nil
}
