package blobstor

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// IterationElement represents a unit of elements through which Iterate operation passes.
type IterationElement struct {
	data []byte

	addr oid.Address

	blzID *blobovnicza.ID
}

// ObjectData returns the stored object in a binary representation.
func (x IterationElement) ObjectData() []byte {
	return x.data
}

// BlobovniczaID returns the identifier of Blobovnicza in which object is stored.
// Returns nil if the object isn't in Blobovnicza.
func (x IterationElement) BlobovniczaID() *blobovnicza.ID {
	return x.blzID
}

// Address returns the object address.
func (x IterationElement) Address() oid.Address {
	return x.addr
}

// IterationHandler is a generic processor of IterationElement.
type IterationHandler func(IterationElement) error

// IteratePrm groups the parameters of Iterate operation.
type IteratePrm struct {
	handler      IterationHandler
	ignoreErrors bool
	errorHandler func(oid.Address, error) error
}

// IterateRes groups the resulting values of Iterate operation.
type IterateRes struct{}

// SetIterationHandler sets the action to be performed on each iteration.
func (i *IteratePrm) SetIterationHandler(h IterationHandler) {
	i.handler = h
}

// IgnoreErrors sets the flag signifying whether errors should be ignored.
func (i *IteratePrm) IgnoreErrors() {
	i.ignoreErrors = true
}

// SetErrorHandler sets error handler for objects that cannot be read or unmarshaled.
func (i *IteratePrm) SetErrorHandler(f func(oid.Address, error) error) {
	i.errorHandler = f
}

// Iterate traverses the storage over the stored objects and calls the handler
// on each element.
//
// Returns any error encountered that
// did not allow to completely iterate over the storage.
//
// If handler returns an error, method wraps and returns it immediately.
func (b *BlobStor) Iterate(prm IteratePrm) (IterateRes, error) {
	var elem IterationElement

	err := b.blobovniczas.Iterate(prm.ignoreErrors, func(p string, blz *blobovnicza.Blobovnicza) error {
		err := blobovnicza.IterateObjects(blz, func(addr oid.Address, data []byte) error {
			var err error

			// decompress the data
			elem.data, err = b.Decompress(data)
			if err != nil {
				if prm.ignoreErrors {
					if prm.errorHandler != nil {
						return prm.errorHandler(addr, err)
					}
					return nil
				}
				return fmt.Errorf("could not decompress object data: %w", err)
			}

			elem.addr = addr
			elem.blzID = blobovnicza.NewIDFromBytes([]byte(p))

			return prm.handler(elem)
		})
		if err != nil {
			return fmt.Errorf("blobovnicza iterator failure %s: %w", p, err)
		}

		return nil
	})
	if err != nil {
		return IterateRes{}, fmt.Errorf("blobovniczas iterator failure: %w", err)
	}

	elem.blzID = nil

	var fsPrm fstree.IterationPrm
	fsPrm.WithIgnoreErrors(prm.ignoreErrors)
	fsPrm.WithHandler(func(addr oid.Address, data []byte) error {
		// decompress the data
		elem.data, err = b.Decompress(data)
		if err != nil {
			if prm.ignoreErrors {
				if prm.errorHandler != nil {
					return prm.errorHandler(addr, err)
				}
				return nil
			}
			return fmt.Errorf("could not decompress object data: %w", err)
		}

		elem.addr = addr

		return prm.handler(elem)
	})

	err = b.fsTree.Iterate(fsPrm)

	if err != nil {
		return IterateRes{}, fmt.Errorf("fs tree iterator failure: %w", err)
	}

	return IterateRes{}, nil
}

// IterateBinaryObjects is a helper function which iterates over BlobStor and passes binary objects to f.
// Errors related to object reading and unmarshaling are logged and skipped.
func IterateBinaryObjects(blz *BlobStor, f func(addr oid.Address, data []byte, blzID *blobovnicza.ID) error) error {
	var prm IteratePrm

	prm.SetIterationHandler(func(elem IterationElement) error {
		return f(elem.Address(), elem.ObjectData(), elem.BlobovniczaID())
	})
	prm.IgnoreErrors()
	prm.SetErrorHandler(func(addr oid.Address, err error) error {
		blz.log.Warn("error occurred during the iteration",
			zap.Stringer("address", addr),
			zap.String("err", err.Error()))
		return nil
	})

	_, err := blz.Iterate(prm)

	return err
}
