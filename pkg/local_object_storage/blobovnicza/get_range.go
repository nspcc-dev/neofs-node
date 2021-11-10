package blobovnicza

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// GetRangePrm groups the parameters of GetRange operation.
type GetRangePrm struct {
	addr *objectSDK.Address

	rng *objectSDK.Range
}

// GetRangeRes groups resulting values of GetRange operation.
type GetRangeRes struct {
	rngData []byte
}

// SetAddress sets address of the requested object.
func (p *GetRangePrm) SetAddress(addr *objectSDK.Address) {
	p.addr = addr
}

// SetRange sets range of the requested payload data .
func (p *GetRangePrm) SetRange(rng *objectSDK.Range) {
	p.rng = rng
}

// RangeData returns the requested payload data range.
func (p *GetRangeRes) RangeData() []byte {
	return p.rngData
}

// GetRange reads range of the object from Blobovnicza by address.
//
// Returns any error encountered that
// did not allow to completely read the object.
//
// Returns ErrNotFound if requested object is not
// presented in Blobovnicza. Returns ErrRangeOutOfBounds
// if requested range is outside the payload.
func (b *Blobovnicza) GetRange(prm *GetRangePrm) (*GetRangeRes, error) {
	res, err := b.Get(&GetPrm{
		addr: prm.addr,
	})
	if err != nil {
		return nil, err
	}

	// FIXME: code below is incorrect because Get returns raw object data
	//  so we should unmarshal payload from it before. If blobovnicza
	//  stores objects in non-protocol format (e.g. compressed)
	//  then it should not provide GetRange method.

	from := prm.rng.GetOffset()
	to := from + prm.rng.GetLength()
	payload := res.obj

	if from > to {
		return nil, fmt.Errorf("invalid range [%d:%d]", from, to)
	} else if uint64(len(payload)) < to {
		return nil, object.ErrRangeOutOfBounds
	}

	return &GetRangeRes{
		rngData: payload[from:to],
	}, nil
}
