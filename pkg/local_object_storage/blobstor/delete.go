package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"go.uber.org/zap"
)

func (b *BlobStor) Delete(prm common.DeletePrm) (common.DeleteRes, error) {
	if prm.StorageID == nil {
		for i := range b.storage {
			res, err := b.storage[i].Storage.Delete(prm)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				if err == nil {
					storagelog.Write(b.log,
						storagelog.AddressField(prm.Address),
						storagelog.OpField("DELETE"),
						zap.String("type", b.storage[i].Storage.Type()),
						zap.String("storage ID", string(prm.StorageID)))
				}
				return res, err
			}
		}
	}
	if len(prm.StorageID) == 0 {
		return b.storage[len(b.storage)-1].Storage.Delete(prm)
	}
	return b.storage[0].Storage.Delete(prm)
}
