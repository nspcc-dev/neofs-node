package blobstor

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
)

// IterationHandler represents the action to be performed on each iteration.
type IterationHandler func(*objectSDK.Address, *blobovnicza.ID) error

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
// If handler returns an error, method returns it immediately.
func (b *BlobStor) Iterate(prm *IteratePrm) (*IterateRes, error) {
	panic("implement me")
}
