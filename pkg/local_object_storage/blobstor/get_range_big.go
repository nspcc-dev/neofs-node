package blobstor

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// GetRangeBigPrm groups the parameters of GetRangeBig operation.
type GetRangeBigPrm struct {
	address
	rwRange
}

// GetRangeBigRes groups the resulting values of GetRangeBig operation.
type GetRangeBigRes struct {
	rangeData
}

// GetRangeBig reads data of object payload range from shallow dir of BLOB storage.
//
// Returns any error encountered that
// did not allow to completely read the object payload range.
//
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
// Returns an error of type apistatus.ObjectNotFound if object is missing.
func (b *BlobStor) GetRangeBig(prm *GetRangeBigPrm) (*GetRangeBigRes, error) {
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

	payload := obj.Payload()
	ln, off := prm.rng.GetLength(), prm.rng.GetOffset()

	if pLen := uint64(len(payload)); pLen < ln+off {
		return nil, object.ErrRangeOutOfBounds
	}

	return &GetRangeBigRes{
		rangeData: rangeData{
			data: payload[off : off+ln],
		},
	}, nil
}
