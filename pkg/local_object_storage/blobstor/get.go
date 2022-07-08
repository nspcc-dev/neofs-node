package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// Get reads the object from b.
// If the descriptor is present, only one sub-storage is tried,
// Otherwise, each sub-storage is tried in order.
func (b *BlobStor) Get(prm common.GetPrm) (common.GetRes, error) {
	if prm.StorageID == nil {
		// Nothing specified, try everything.
		res, err := b.getBig(prm)
		if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
			return res, err
		}
		return b.getSmall(prm)
	}
	if len(prm.StorageID) == 0 {
		return b.getBig(prm)
	}
	return b.getSmall(prm)
}

// getBig reads the object from shallow dir of BLOB storage by address.
//
// Returns any error encountered that
// did not allow to completely read the object.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is not
// presented in shallow dir.
func (b *BlobStor) getBig(prm common.GetPrm) (common.GetRes, error) {
	// get compressed object data
	return b.fsTree.Get(prm)
}

func (b *BlobStor) getSmall(prm common.GetPrm) (common.GetRes, error) {
	return b.blobovniczas.Get(prm)
}
