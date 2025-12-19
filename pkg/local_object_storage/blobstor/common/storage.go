package common

import (
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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
	Get(oid.Address) (*object.Object, error)
	GetRange(oid.Address, uint64, uint64) ([]byte, error)
	GetStream(oid.Address) (*object.Object, io.ReadCloser, error)
	Head(oid.Address) (*object.Object, error)
	Exists(oid.Address) (bool, error)
	Put(oid.Address, []byte) error
	PutBatch(map[oid.Address][]byte) error
	Delete(oid.Address) error
	Iterate(func(oid.Address, []byte) error, func(oid.Address, error) error) error
	IterateAddresses(func(oid.Address) error, bool) error
}

// Copy copies all objects from source Storage into the destination one. If any
// object cannot be stored, Copy immediately fails.
func Copy(dst, src Storage) error {
	return CopyBatched(dst, src, 0)
}

// CopyBatched copies all objects from source Storage into the destination one
// using batched API. If any batch cannot be stored, Copy immediately fails.
// batchSize less than or equal to 1 makes it the same as [Copy].
func CopyBatched(dst, src Storage, batchSize int) error {
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

	var objBatch = make(map[oid.Address][]byte)

	err = src.Iterate(func(addr oid.Address, data []byte) error {
		exists, err := dst.Exists(addr)
		if err != nil {
			return fmt.Errorf("check presence of object %s in the destination sub-storage: %w", addr, err)
		} else if exists {
			return nil
		}

		if batchSize <= 1 {
			err = dst.Put(addr, data)
			if err != nil {
				return fmt.Errorf("put object %s into destination sub-storage: %w", addr, err)
			}
			return nil
		}
		objBatch[addr] = data
		if len(objBatch) == batchSize {
			err = dst.PutBatch(objBatch)
			if err != nil {
				return fmt.Errorf("put batch into destination sub-storage: %w", err)
			}
			clear(objBatch)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("iterate over source sub-storage: %w", err)
	}

	if len(objBatch) > 0 {
		err = dst.PutBatch(objBatch)
		if err != nil {
			return fmt.Errorf("put batch into destination sub-storage: %w", err)
		}
	}
	return nil
}
