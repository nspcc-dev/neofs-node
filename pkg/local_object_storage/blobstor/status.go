package blobstor

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectSubstorageStatus represents the status of the object in the Blob
// storage, containing the type and path of the storage and an error if it
// occurred.
type ObjectSubstorageStatus struct {
	Type  string
	Path  string
	Error error
}

// ObjectStatus represents the status of the object in the Blob storage.
type ObjectStatus struct {
	Substorages []ObjectSubstorageStatus
}

// ObjectStatus returns the status of the object in the Blob storage. It contains
// status of the object in all blob substorages.
func (b *BlobStor) ObjectStatus(address oid.Address) (ObjectStatus, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()
	res := ObjectStatus{
		Substorages: []ObjectSubstorageStatus{},
	}
	prm := common.GetPrm{
		Address: address,
	}
	for i := range b.storage {
		_, err := b.storage[i].Storage.Get(prm)
		if err == nil {
			res.Substorages = append(res.Substorages, ObjectSubstorageStatus{
				Type: b.storage[i].Storage.Type(),
				Path: b.storage[i].Storage.Path(),
			})
		}
	}
	return res, nil
}
