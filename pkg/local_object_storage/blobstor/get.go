package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Get reads the object from b.
// If the descriptor is present, only one sub-storage is tried,
// Otherwise, each sub-storage is tried in order.
func (b *BlobStor) Get(addr oid.Address) (*objectSDK.Object, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	for i := range b.storage {
		obj, err := b.storage[i].Storage.Get(addr)
		if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
			return obj, err
		}
	}

	return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
}

// GetBytes reads object from the BlobStor by address into memory buffer in a
// canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
// is missing.
func (b *BlobStor) GetBytes(addr oid.Address) ([]byte, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	var bs []byte
	for i := range b.storage {
		var err error
		bs, err = b.storage[i].Storage.GetBytes(addr)
		if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
			return bs, err
		}
	}

	return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
}
