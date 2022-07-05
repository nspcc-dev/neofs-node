package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// DeleteBigPrm groups the parameters of DeleteBig operation.
type DeleteBigPrm struct {
	address
}

// DeleteBigRes groups the resulting values of DeleteBig operation.
type DeleteBigRes struct{}

// DeleteBig removes an object from shallow dir of BLOB storage.
//
// Returns any error encountered that did not allow
// to completely remove the object.
//
// Returns an error of type apistatus.ObjectNotFound if there is no object to delete.
func (b *BlobStor) DeleteBig(prm DeleteBigPrm) (DeleteBigRes, error) {
	err := b.fsTree.Delete(prm.addr)
	if errors.Is(err, fstree.ErrFileNotFound) {
		var errNotFound apistatus.ObjectNotFound

		err = errNotFound
	}

	if err == nil {
		storagelog.Write(b.log, storagelog.AddressField(prm.addr), storagelog.OpField("fstree DELETE"))
	}

	return DeleteBigRes{}, err
}

// DeleteSmall removes an object from blobovnicza of BLOB storage.
//
// If blobovnicza ID is not set or set to nil, BlobStor tries to
// find and remove object from any blobovnicza.
//
// Returns any error encountered that did not allow
// to completely remove the object.
//
// Returns an error of type apistatus.ObjectNotFound if there is no object to delete.
func (b *BlobStor) DeleteSmall(prm blobovniczatree.DeleteSmallPrm) (blobovniczatree.DeleteSmallRes, error) {
	return b.blobovniczas.Delete(prm)
}
