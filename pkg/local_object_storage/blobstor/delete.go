package blobstor

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

func (b *BlobStor) Delete(addr oid.Address) error {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	for i := range b.storage {
		err := b.storage[i].Storage.Delete(addr)
		if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
			if err == nil {
				logOp(b.log, deleteOp, addr, b.storage[i].Storage.Type())
			}
			return err
		}
	}

	return nil
}
