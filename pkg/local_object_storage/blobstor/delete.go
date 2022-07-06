package blobstor

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

func (b *BlobStor) Delete(prm common.DeletePrm) (common.DeleteRes, error) {
	if prm.StorageID == nil {
		// Nothing specified, try everything.
		res, err := b.deleteBig(prm)
		if err == nil || !errors.As(err, new(apistatus.ObjectNotFound)) {
			return res, err
		}
		return b.deleteSmall(prm)
	}
	if len(prm.StorageID) == 0 {
		return b.deleteBig(prm)
	}
	return b.deleteSmall(prm)
}

// deleteBig removes an object from shallow dir of BLOB storage.
//
// Returns any error encountered that did not allow
// to completely remove the object.
//
// Returns an error of type apistatus.ObjectNotFound if there is no object to delete.
func (b *BlobStor) deleteBig(prm common.DeletePrm) (common.DeleteRes, error) {
	err := b.fsTree.Delete(prm.Address)
	if errors.Is(err, fstree.ErrFileNotFound) {
		var errNotFound apistatus.ObjectNotFound

		err = errNotFound
	}

	if err == nil {
		storagelog.Write(b.log, storagelog.AddressField(prm.Address), storagelog.OpField("fstree DELETE"))
	}

	return common.DeleteRes{}, err
}

// deleteSmall removes an object from blobovnicza of BLOB storage.
//
// If blobovnicza ID is not set or set to nil, BlobStor tries to
// find and remove object from any blobovnicza.
//
// Returns any error encountered that did not allow
// to completely remove the object.
//
// Returns an error of type apistatus.ObjectNotFound if there is no object to delete.
func (b *BlobStor) deleteSmall(prm common.DeletePrm) (common.DeleteRes, error) {
	return b.blobovniczas.Delete(prm)
}
