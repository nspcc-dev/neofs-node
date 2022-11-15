package blobstor

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// ErrNoPlaceFound is returned when object can't be saved to any sub-storage component
// because of the policy.
var ErrNoPlaceFound = logicerr.New("couldn't find a place to store an object")

// Put saves the object in BLOB storage.
//
// If object is "big", BlobStor saves the object in shallow dir.
// Otherwise, BlobStor saves the object in blobonicza. In this
// case the identifier of blobovnicza is returned.
//
// Returns any error encountered that
// did not allow to completely save the object.
func (b *BlobStor) Put(prm common.PutPrm) (common.PutRes, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if prm.Object != nil {
		prm.Address = object.AddressOf(prm.Object)
	}
	if prm.RawData == nil {
		// marshal object
		data, err := prm.Object.Marshal()
		if err != nil {
			return common.PutRes{}, fmt.Errorf("could not marshal the object: %w", err)
		}
		prm.RawData = data
	}

	for i := range b.storage {
		if b.storage[i].Policy == nil || b.storage[i].Policy(prm.Object, prm.RawData) {
			res, err := b.storage[i].Storage.Put(prm)
			if err == nil {
				logOp(b.log, putOp, prm.Address, b.storage[i].Storage.Type(), res.StorageID)
			}
			return res, err
		}
	}

	return common.PutRes{}, ErrNoPlaceFound
}

// NeedsCompression returns true if the object should be compressed.
// For an object to be compressed 2 conditions must hold:
// 1. Compression is enabled in settings.
// 2. Object MIME Content-Type is allowed for compression.
func (b *BlobStor) NeedsCompression(obj *objectSDK.Object) bool {
	return b.cfg.compression.NeedsCompression(obj)
}
