package common

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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
	Get(oid.Address) (*objectSDK.Object, error)
	GetRange(oid.Address, uint64, uint64) ([]byte, error)
	Exists(oid.Address) (bool, error)
	Put(oid.Address, []byte) error
	Delete(oid.Address) error
	Iterate(func(oid.Address, []byte, []byte) error, func(oid.Address, error) error) error
	IterateLazily(func(oid.Address, func() ([]byte, error)) error, bool) error
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

	err = src.Iterate(func(addr oid.Address, data []byte, _ []byte) error {
		exists, err := dst.Exists(addr)
		if err != nil {
			return fmt.Errorf("check presence of object %s in the destination sub-storage: %w", addr, err)
		} else if exists {
			return nil
		}

		err = dst.Put(addr, data)
		if err != nil {
			return fmt.Errorf("put object %s into destination sub-storage: %w", addr, err)
		}
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("iterate over source sub-storage: %w", err)
	}

	return nil
}
