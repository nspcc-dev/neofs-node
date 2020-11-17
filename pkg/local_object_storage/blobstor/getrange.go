package blobstor

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/pkg/errors"
)

// GetRangePrm groups the parameters of GetRange operation.
type GetRangePrm struct {
	off, ln uint64

	addr *objectSDK.Address
}

// GetRangeRes groups resulting values of GetRange operation.
type GetRangeRes struct {
	rngData []byte
}

// WithAddress is a GetRange option to set the address of the requested object.
//
// Option is required.
func (p *GetRangePrm) WithAddress(addr *objectSDK.Address) *GetRangePrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// WithPayloadRange is a GetRange option to set range of requested payload data.
//
// Option is required.
func (p *GetRangePrm) WithPayloadRange(off, ln uint64) *GetRangePrm {
	if p != nil {
		p.off, p.ln = off, ln
	}

	return p
}

// RangeData returns data of the requested payload range.
func (r *GetRangeRes) RangeData() []byte {
	return r.rngData
}

// GetRange reads data of object payload range from BLOB storage.
//
// Returns any error encountered that
// did not allow to completely read the object payload range.
func (b *BlobStor) GetRange(prm *GetRangePrm) (*GetRangeRes, error) {
	b.mtx.RLock()
	defer b.mtx.RUnlock()

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
	if pLen := uint64(len(payload)); pLen < prm.ln+prm.off {
		return nil, errors.Errorf("range is out-of-bounds (payload %d, off %d, ln %d)",
			pLen, prm.off, prm.ln)
	}

	return &GetRangeRes{
		rngData: payload[prm.off : prm.off+prm.ln],
	}, nil
}
