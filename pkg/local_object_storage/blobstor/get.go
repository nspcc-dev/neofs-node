package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Get reads the object from b.
// If the descriptor is present, only one sub-storage is tried,
// Otherwise, each sub-storage is tried in order.
func (b *BlobStor) Get(prm common.GetPrm) (common.GetRes, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if prm.StorageID == nil {
		for i := range b.storage {
			res, err := b.storage[i].Storage.Get(prm)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				return res, err
			}
		}

		return common.GetRes{}, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	if len(prm.StorageID) == 0 {
		return b.storage[len(b.storage)-1].Storage.Get(prm)
	}
	return b.storage[0].Storage.Get(prm)
}

// GetBytes reads object from the BlobStor by address into memory buffer in a
// canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
// is missing.
func (b *BlobStor) GetBytes(addr oid.Address, subStorageID []byte) ([]byte, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	var bs []byte
	if subStorageID == nil {
		for i := range b.storage {
			var err error
			bs, err = b.storage[i].Storage.GetBytes(addr)
			if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
				return bs, err
			}
		}

		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}
	if len(subStorageID) == 0 {
		return b.storage[len(b.storage)-1].Storage.GetBytes(addr)
	}
	return b.storage[0].Storage.GetBytes(addr)
}
