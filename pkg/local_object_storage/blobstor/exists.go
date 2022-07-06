package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"go.uber.org/zap"
)

// Exists checks if the object is presented in BLOB storage.
//
// Returns any error encountered that did not allow
// to completely check object existence.
func (b *BlobStor) Exists(prm common.ExistsPrm) (common.ExistsRes, error) {
	// check presence in shallow dir first (cheaper)
	res, err := b.existsBig(prm)

	// If there was an error during existence check below,
	// it will be returned unless object was found in blobovnicza.
	// Otherwise, it is logged and the latest error is returned.
	// FSTree    | Blobovnicza | Behaviour
	// found     | (not tried) | return true, nil
	// not found | any result  | return the result
	// error     | found       | log the error, return true, nil
	// error     | not found   | return the error
	// error     | error       | log the first error, return the second
	if !res.Exists {
		var smallErr error

		res, smallErr = b.existsSmall(prm)
		if err != nil && (smallErr != nil || res.Exists) {
			b.log.Warn("error occured during object existence checking",
				zap.Stringer("address", prm.Address),
				zap.String("error", err.Error()))
			err = nil
		}
		if err == nil {
			err = smallErr
		}
	}

	return res, err
}

// checks if object is presented in shallow dir.
func (b *BlobStor) existsBig(prm common.ExistsPrm) (common.ExistsRes, error) {
	_, err := b.fsTree.Exists(prm.Address)
	if errors.Is(err, fstree.ErrFileNotFound) {
		return common.ExistsRes{}, nil
	}

	return common.ExistsRes{Exists: err == nil}, err
}

// existsSmall checks if object is presented in blobovnicza.
func (b *BlobStor) existsSmall(prm common.ExistsPrm) (common.ExistsRes, error) {
	return b.blobovniczas.Exists(prm)
}
