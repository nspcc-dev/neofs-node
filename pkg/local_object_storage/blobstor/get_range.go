package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// GetRange reads object payload data from b.
// If the descriptor is present, only one sub-storage is tried,
// Otherwise, each sub-storage is tried in order.
func (b *BlobStor) GetRange(prm common.GetRangePrm) (common.GetRangeRes, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if prm.StorageID == nil {
		for i := range b.storage {
			res, err := b.storage[i].Storage.GetRange(prm)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				return res, err
			}
		}

		return common.GetRangeRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	if len(prm.StorageID) == 0 {
		return b.storage[len(b.storage)-1].Storage.GetRange(prm)
	}
	return b.storage[0].Storage.GetRange(prm)
}
