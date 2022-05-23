package blobstor

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// GetBigPrm groups the parameters of GetBig operation.
type GetBigPrm struct {
	address
}

// GetBigRes groups the resulting values of GetBig operation.
type GetBigRes struct {
	roObject
}

// GetBig reads the object from shallow dir of BLOB storage by address.
//
// Returns any error encountered that
// did not allow to completely read the object.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is not
// presented in shallow dir.
func (b *BlobStor) GetBig(prm GetBigPrm) (*GetBigRes, error) {
	// get compressed object data
	data, err := b.fsTree.Get(prm.addr)
	if err != nil {
		if errors.Is(err, fstree.ErrFileNotFound) {
			var errNotFound apistatus.ObjectNotFound

			return nil, errNotFound
		}

		return nil, fmt.Errorf("could not read object from fs tree: %w", err)
	}

	data, err = b.decompressor(data)
	if err != nil {
		return nil, fmt.Errorf("could not decompress object data: %w", err)
	}

	// unmarshal the object
	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, fmt.Errorf("could not unmarshal the object: %w", err)
	}

	return &GetBigRes{
		roObject: roObject{
			obj: obj,
		},
	}, nil
}
