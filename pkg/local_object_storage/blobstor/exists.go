package blobstor

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Exists checks if the object is presented in BLOB storage.
//
// Returns any error encountered that did not allow
// to completely check object existence.
func (b *BlobStor) Exists(addr oid.Address) (bool, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	exists, err := b.storage.Storage.Exists(addr)
	if err != nil {
		return false, err
	}

	return exists, nil
}
