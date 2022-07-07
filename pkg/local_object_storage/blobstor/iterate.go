package blobstor

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Iterate traverses the storage over the stored objects and calls the handler
// on each element.
//
// Returns any error encountered that
// did not allow to completely iterate over the storage.
//
// If handler returns an error, method wraps and returns it immediately.
func (b *BlobStor) Iterate(prm common.IteratePrm) (common.IterateRes, error) {
	_, err := b.blobovniczas.Iterate(prm)
	if err != nil && !prm.IgnoreErrors {
		return common.IterateRes{}, fmt.Errorf("blobovnizas iterator failure: %w", err)
	}

	// FIXME decompress in the fstree
	iPrm := prm
	iPrm.Handler = func(element common.IterationElement) error {
		data, err := b.Decompress(element.ObjectData)
		if err != nil {
			if prm.IgnoreErrors {
				if prm.ErrorHandler != nil {
					return prm.ErrorHandler(element.Address, err)
				}
				return nil
			}
			return fmt.Errorf("could not decompress object data: %w", err)
		}
		element.ObjectData = data
		return prm.Handler(element)
	}

	_, err = b.fsTree.Iterate(iPrm)
	if err != nil && !prm.IgnoreErrors {
		return common.IterateRes{}, fmt.Errorf("fs tree iterator failure: %w", err)
	}
	return common.IterateRes{}, nil
}

// IterateBinaryObjects is a helper function which iterates over BlobStor and passes binary objects to f.
// Errors related to object reading and unmarshaling are logged and skipped.
func IterateBinaryObjects(blz *BlobStor, f func(addr oid.Address, data []byte, descriptor []byte) error) error {
	var prm common.IteratePrm

	prm.Handler = func(elem common.IterationElement) error {
		return f(elem.Address, elem.ObjectData, elem.StorageID)
	}
	prm.IgnoreErrors = true
	prm.ErrorHandler = func(addr oid.Address, err error) error {
		blz.log.Warn("error occurred during the iteration",
			zap.Stringer("address", addr),
			zap.String("err", err.Error()))
		return nil
	}

	_, err := blz.Iterate(prm)

	return err
}
