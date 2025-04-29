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
//
// Zero length is interpreted as requiring full object length independent of the
// offset.
func (b *BlobStor) GetRange(addr oid.Address, offset uint64, length uint64) ([]byte, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	res, err := b.storage.Storage.GetRange(addr, offset, length)
	if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
		return res, err
	}

	return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
}
