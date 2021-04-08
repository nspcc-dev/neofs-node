package blobstor

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/pkg/errors"
)

// DeleteBigPrm groups the parameters of DeleteBig operation.
type DeleteBigPrm struct {
	address
}

// DeleteBigRes groups resulting values of DeleteBig operation.
type DeleteBigRes struct{}

// DeleteBig removes object from shallow dir of BLOB storage.
//
// Returns any error encountered that did not allow
// to completely remove the object.
//
// Returns ErrNotFound if there is no object to delete.
func (b *BlobStor) DeleteBig(prm *DeleteBigPrm) (*DeleteBigRes, error) {
	err := b.fsTree.Delete(prm.addr)
	if errors.Is(err, fstree.ErrFileNotFound) {
		err = object.ErrNotFound
	}

	return nil, err
}
