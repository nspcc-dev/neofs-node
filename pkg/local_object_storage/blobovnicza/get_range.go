package blobovnicza

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/pkg/errors"
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

var ErrRangeOutOfBounds = errors.New("payload range is out of bounds")

// SetAddress sets address of the requested object.
func (p *GetRangePrm) SetAddress(addr *objectSDK.Address) {
	p.addr = addr
}

// SetAddress sets range of the requested payload data .
func (p *GetRangePrm) SetRange(rng *objectSDK.Range) {
	p.rng = rng
}

// RangeData returns the requested payload data range.
func (p *GetRangeRes) RangeData() []byte {
	return p.rngData
}

// Get reads the object from Blobovnicza by address.
//
// Returns any error encountered that
// did not allow to completely read the object.
func (b *Blobovnicza) GetRange(prm *GetRangePrm) (*GetRangeRes, error) {
	res, err := b.Get(&GetPrm{
		addr: prm.addr,
	})
	if err != nil {
		return nil, err
	}

	from := prm.rng.GetOffset()
	to := from + prm.rng.GetLength()
	payload := res.obj.Payload()

	if from > to {
		return nil, errors.Errorf("invalid range [%d:%d]", from, to)
	} else if uint64(len(payload)) < to {
		return nil, ErrRangeOutOfBounds
	}

	return &GetRangeRes{
		rngData: payload[from:to],
	}, nil
}
