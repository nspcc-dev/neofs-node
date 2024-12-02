package blobstor

import (
	"fmt"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Iterate traverses the storage over the stored objects and calls the objHandler
// on each element. errorHandler is called if specified and ignoreErrors is true.
//
// Returns any error encountered that
// did not allow to completely iterate over the storage.
//
// If handler returns an error, method wraps and returns it immediately.
func (b *BlobStor) Iterate(objHandler func(addr oid.Address, data []byte, id []byte) error, errorHandler func(addr oid.Address, err error) error, ignoreErrors bool) error {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	for i := range b.storage {
		err := b.storage[i].Storage.Iterate(objHandler, errorHandler, ignoreErrors)
		if err != nil && !ignoreErrors {
			return fmt.Errorf("blobstor iterator failure: %w", err)
		}
	}
	return nil
}

// IterateBinaryObjects is a helper method which iterates over BlobStor and passes binary objects to f.
// Errors related to object reading and unmarshaling are logged and skipped.
func (b *BlobStor) IterateBinaryObjects(f func(addr oid.Address, data []byte, descriptor []byte) error) error {
	var errorHandler = func(addr oid.Address, err error) error {
		b.log.Warn("error occurred during the iteration",
			zap.Stringer("address", addr),
			zap.String("err", err.Error()))
		return nil
	}

	return b.Iterate(f, errorHandler, true)
}
