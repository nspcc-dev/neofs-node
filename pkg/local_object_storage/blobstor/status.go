package blobstor

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectStatus represents the status of the object in the Blob
// storage, containing the type and path of the storage and an error if it
// occurred.
type ObjectStatus struct {
	Type  string
	Path  string
	Error error
}

// ObjectStatus returns the status of the object in the Blob storage. It contains
// status of the object in all blob substorages.
func (b *BlobStor) ObjectStatus(address oid.Address) (ObjectStatus, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	var res ObjectStatus
	_, err := b.storage.Storage.Get(address)
	if err == nil {
		res = ObjectStatus{
			Type: b.storage.Storage.Type(),
			Path: b.storage.Storage.Path(),
		}
	}
	return res, nil
}
