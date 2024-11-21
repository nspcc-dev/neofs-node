package blobstor

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"go.uber.org/zap"
)

// Exists checks if the object is presented in BLOB storage.
//
// Returns any error encountered that did not allow
// to completely check object existence.
func (b *BlobStor) Exists(prm common.ExistsPrm) (common.ExistsRes, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

	if prm.StorageID != nil {
		if len(prm.StorageID) == 0 {
			return b.storage[len(b.storage)-1].Storage.Exists(prm)
		}
		return b.storage[0].Storage.Exists(prm)
	}

	// If there was an error during existence check below,
	// it will be returned unless object was found in blobovnicza.
	// Otherwise, it is logged and the latest error is returned.
	// FSTree    | Blobovnicza | Behaviour
	// found     | (not tried) | return true, nil
	// not found | any result  | return the result
	// error     | found       | log the error, return true, nil
	// error     | not found   | return the error
	// error     | error       | log the first error, return the second
	var errors []error
	for i := range b.storage {
		res, err := b.storage[i].Storage.Exists(prm)
		if err == nil && res.Exists {
			return res, nil
		} else if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 {
		return common.ExistsRes{}, nil
	}

	for _, err := range errors[:len(errors)-1] {
		b.log.Warn("error occurred during object existence checking",
			zap.Stringer("address", prm.Address),
			zap.Error(err))
	}

	return common.ExistsRes{}, errors[len(errors)-1]
}
