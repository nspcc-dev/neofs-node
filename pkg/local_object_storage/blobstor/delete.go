package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func (b *BlobStor) Delete(addr oid.Address, storageID []byte) error {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if storageID == nil {
		for i := range b.storage {
			err := b.storage[i].Storage.Delete(addr)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				if err == nil {
					logOp(b.log, deleteOp, addr, b.storage[i].Storage.Type(), storageID)
				}
				return err
			}
		}
	}

	var st common.Storage

	if len(storageID) == 0 {
		st = b.storage[len(b.storage)-1].Storage
	} else {
		st = b.storage[0].Storage
	}

	err := st.Delete(addr)
	if err == nil {
		logOp(b.log, deleteOp, addr, st.Type(), storageID)
	}

	return err
}
