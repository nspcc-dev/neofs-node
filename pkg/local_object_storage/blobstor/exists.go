package blobstor

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Exists checks if the object is presented in BLOB storage.
//
// Returns any error encountered that did not allow
// to completely check object existence.
func (b *BlobStor) Exists(addr oid.Address) (bool, error) {
	b.modeMtx.RLock()
	defer b.modeMtx.RUnlock()

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
		exists, err := b.storage[i].Storage.Exists(addr)
		if err == nil && exists {
			return exists, nil
		} else if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 {
		return false, nil
	}

	for _, err := range errors[:len(errors)-1] {
		b.log.Warn("error occurred during object existence checking",
			zap.Stringer("address", addr),
			zap.Error(err))
	}

	return false, errors[len(errors)-1]
}
