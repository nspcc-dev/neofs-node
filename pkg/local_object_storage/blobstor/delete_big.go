package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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
// Returns an error of type apistatus.ObjectNotFound if there is no object to delete.
func (b *BlobStor) DeleteBig(prm *DeleteBigPrm) (*DeleteBigRes, error) {
	err := b.fsTree.Delete(prm.addr)
	if errors.Is(err, fstree.ErrFileNotFound) {
		var errNotFound apistatus.ObjectNotFound

		err = errNotFound
	}

	if err == nil {
		storagelog.Write(b.log, storagelog.AddressField(prm.addr), storagelog.OpField("fstree DELETE"))
	}

	return nil, err
}
