package blobstor

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// GetBig reads the object from shallow dir of BLOB storage by address.
//
// Returns any error encountered that
// did not allow to completely read the object.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is not
// presented in shallow dir.
func (b *BlobStor) GetBig(prm common.GetPrm) (common.GetRes, error) {
	// get compressed object data
	data, err := b.fsTree.Get(prm)
	if err != nil {
		if errors.Is(err, fstree.ErrFileNotFound) {
			var errNotFound apistatus.ObjectNotFound

			return common.GetRes{}, errNotFound
		}

		return common.GetRes{}, fmt.Errorf("could not read object from fs tree: %w", err)
	}

	data, err = b.Decompress(data)
	if err != nil {
		return common.GetRes{}, fmt.Errorf("could not decompress object data: %w", err)
	}

	// unmarshal the object
	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return common.GetRes{}, fmt.Errorf("could not unmarshal the object: %w", err)
	}

	return common.GetRes{Object: obj}, nil
}

func (b *BlobStor) GetSmall(prm common.GetPrm) (common.GetRes, error) {
	return b.blobovniczas.Get(prm)
}
