package blobstor

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
)

// GetRangeBigPrm groups the parameters of GetRangeBig operation.
type GetRangeBigPrm struct {
	address
	rwRange
}

// GetRangeBigRes groups resulting values of GetRangeBig operation.
type GetRangeBigRes struct {
	rangeData
}

// GetRangeBig reads data of object payload range from shallow dir of BLOB storage.
//
// Returns any error encountered that
// did not allow to completely read the object payload range.
func (b *BlobStor) GetRangeBig(prm *GetRangeBigPrm) (*GetRangeBigRes, error) {
	// get compressed object data
	data, err := b.fsTree.get(prm.addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not read object from fs tree")
	}

	data, err = b.decompressor(data)
	if err != nil {
		return nil, errors.Wrap(err, "could not decompress object data")
	}

	// unmarshal the object
	obj := object.New()
	if err := obj.Unmarshal(data); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal the object")
	}

	payload := obj.Payload()
	ln, off := prm.rng.GetLength(), prm.rng.GetOffset()

	if pLen := uint64(len(payload)); pLen < ln+off {
		return nil, errors.Errorf("range is out-of-bounds (payload %d, off %d, ln %d)",
			pLen, off, ln)
	}

	return &GetRangeBigRes{
		rangeData: rangeData{
			data: payload[off : off+ln],
		},
	}, nil
}
