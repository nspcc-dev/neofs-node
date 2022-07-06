package blobstor

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// GetRangeBig reads data of object payload range from shallow dir of BLOB storage.
//
// Returns any error encountered that
// did not allow to completely read the object payload range.
//
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
// Returns an error of type apistatus.ObjectNotFound if object is missing.
func (b *BlobStor) GetRangeBig(prm common.GetRangePrm) (common.GetRangeRes, error) {
	// get compressed object data
	data, err := b.fsTree.Get(common.GetPrm{Address: prm.Address})
	if err != nil {
		if errors.Is(err, fstree.ErrFileNotFound) {
			var errNotFound apistatus.ObjectNotFound

			return common.GetRangeRes{}, errNotFound
		}

		return common.GetRangeRes{}, fmt.Errorf("could not read object from fs tree: %w", err)
	}

	data, err = b.Decompress(data)
	if err != nil {
		return common.GetRangeRes{}, fmt.Errorf("could not decompress object data: %w", err)
	}

	// unmarshal the object
	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return common.GetRangeRes{}, fmt.Errorf("could not unmarshal the object: %w", err)
	}

	payload := obj.Payload()
	ln, off := prm.Range.GetLength(), prm.Range.GetOffset()

	if pLen := uint64(len(payload)); ln+off < off || pLen < off || pLen < ln+off {
		var errOutOfRange apistatus.ObjectOutOfRange

		return common.GetRangeRes{}, errOutOfRange
	}

	return common.GetRangeRes{
		Data: payload[off : off+ln],
	}, nil
}

// GetRangeSmall reads data of object payload range from blobovnicza of BLOB storage.
//
// If blobovnicza ID is not set or set to nil, BlobStor tries to get payload range
// from any blobovnicza.
//
// Returns any error encountered that
// did not allow to completely read the object payload range.
//
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in blobovnicza(s).
func (b *BlobStor) GetRangeSmall(prm common.GetRangePrm) (common.GetRangeRes, error) {
	return b.blobovniczas.GetRange(prm)
}
