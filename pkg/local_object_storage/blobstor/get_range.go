package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// GetRange reads object payload data from b.
// If the descriptor is present, only one sub-storage is tried,
// Otherwise, each sub-storage is tried in order.
func (b *BlobStor) GetRange(addr oid.Address, offset uint64, length uint64, storageID []byte) ([]byte, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if storageID == nil {
		for i := range b.storage {
			res, err := b.storage[i].Storage.GetRange(addr, offset, length)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				return res, err
			}
		}

		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	if len(storageID) == 0 {
		return b.storage[len(b.storage)-1].Storage.GetRange(addr, offset, length)
	}
	return b.storage[0].Storage.GetRange(addr, offset, length)
}
