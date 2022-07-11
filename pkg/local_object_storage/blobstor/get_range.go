package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// GetRange reads object payload data from b.
// If the descriptor is present, only one sub-storage is tried,
// Otherwise, each sub-storage is tried in order.
func (b *BlobStor) GetRange(prm common.GetRangePrm) (common.GetRangeRes, error) {
	if prm.StorageID == nil {
		for i := range b.storage {
			res, err := b.storage[i].Storage.GetRange(prm)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				return res, err
			}
		}

		var errNotFound apistatus.ObjectNotFound
		return common.GetRangeRes{}, errNotFound
	}
	if len(prm.StorageID) == 0 {
		return b.storage[len(b.storage)-1].Storage.GetRange(prm)
	}
	return b.storage[0].Storage.GetRange(prm)
}
