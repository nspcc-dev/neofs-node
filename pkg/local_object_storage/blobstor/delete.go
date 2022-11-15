package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

func (b *BlobStor) Delete(prm common.DeletePrm) (common.DeleteRes, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if prm.StorageID == nil {
		for i := range b.storage {
			res, err := b.storage[i].Storage.Delete(prm)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				if err == nil {
					logOp(b.log, deleteOp, prm.Address, b.storage[i].Storage.Type(), prm.StorageID)
				}
				return res, err
			}
		}
	}

	var st common.Storage

	if len(prm.StorageID) == 0 {
		st = b.storage[len(b.storage)-1].Storage
	} else {
		st = b.storage[0].Storage
	}

	res, err := st.Delete(prm)
	if err == nil {
		logOp(b.log, deleteOp, prm.Address, st.Type(), prm.StorageID)
	}

	return res, err
}
