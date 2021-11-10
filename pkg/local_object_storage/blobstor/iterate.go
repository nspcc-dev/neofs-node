package blobstor

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// IterationElement represents a unit of elements through which Iterate operation passes.
type IterationElement struct {
	data []byte

	blzID *blobovnicza.ID
}

// ObjectData returns stored object in a binary representation.
func (x IterationElement) ObjectData() []byte {
	return x.data
}

// BlobovniczaID returns identifier of Blobovnicza in which object is stored.
// Returns nil if object isn't in Blobovnicza.
func (x IterationElement) BlobovniczaID() *blobovnicza.ID {
	return x.blzID
}

// IterationHandler is a generic processor of IterationElement.
type IterationHandler func(IterationElement) error

// IteratePrm groups the parameters of Iterate operation.
type IteratePrm struct {
	handler IterationHandler
}

// IterateRes groups resulting values of Iterate operation.
type IterateRes struct{}

// SetIterationHandler sets the action to be performed on each iteration.
func (i *IteratePrm) SetIterationHandler(h IterationHandler) {
	i.handler = h
}

// Iterate traverses the storage over the stored objects and calls the handler
// on each element.
//
// Returns any error encountered that
// did not allow to completely iterate over the storage.
//
// If handler returns an error, method wraps and returns it immediately.
func (b *BlobStor) Iterate(prm IteratePrm) (*IterateRes, error) {
	var elem IterationElement

	err := b.blobovniczas.iterateBlobovniczas(func(p string, blz *blobovnicza.Blobovnicza) error {
		err := blobovnicza.IterateObjects(blz, func(data []byte) error {
			var err error

			// decompress the data
			elem.data, err = b.decompressor(data)
			if err != nil {
				return fmt.Errorf("could not decompress object data: %w", err)
			}

			elem.blzID = blobovnicza.NewIDFromBytes([]byte(p))

			return prm.handler(elem)
		})
		if err != nil {
			return fmt.Errorf("blobovnicza iterator failure %s: %w", p, err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("blobovniczas iterator failure: %w", err)
	}

	elem.blzID = nil

	err = b.fsTree.Iterate(func(_ *objectSDK.Address, data []byte) error {
		// decompress the data
		elem.data, err = b.decompressor(data)
		if err != nil {
			return fmt.Errorf("could not decompress object data: %w", err)
		}

		return prm.handler(elem)
	})
	if err != nil {
		return nil, fmt.Errorf("fs tree iterator failure: %w", err)
	}

	return new(IterateRes), nil
}

// IterateBinaryObjects is a helper function which iterates over BlobStor and passes binary objects to f.
func IterateBinaryObjects(blz *BlobStor, f func(data []byte, blzID *blobovnicza.ID) error) error {
	var prm IteratePrm

	prm.SetIterationHandler(func(elem IterationElement) error {
		return f(elem.ObjectData(), elem.BlobovniczaID())
	})

	_, err := blz.Iterate(prm)

	return err
}

// IterateObjects is a helper function which iterates over BlobStor and passes decoded objects to f.
func IterateObjects(blz *BlobStor, f func(obj *object.Object, blzID *blobovnicza.ID) error) error {
	var obj *object.Object

	return IterateBinaryObjects(blz, func(data []byte, blzID *blobovnicza.ID) error {
		if obj == nil {
			obj = object.New()
		}

		if err := obj.Unmarshal(data); err != nil {
			return fmt.Errorf("could not unmarshal the object: %w", err)
		}

		return f(obj, blzID)
	})
}
