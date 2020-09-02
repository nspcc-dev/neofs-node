package object

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

type Address struct {
	*object.Address
}

// MarshalStableV2 marshals Address to v2 binary format.
func (a *Address) MarshalStableV2() ([]byte, error) {
	if a != nil {
		return a.ToV2().StableMarshal(nil)
	}

	return nil, nil
}

// AddressFromV2 converts v2 Address message to Address.
func AddressFromV2(aV2 *refs.Address) (*Address, error) {
	if aV2 == nil {
		return nil, nil
	}

	a, err := object.AddressFromV2(aV2)
	if err != nil {
		return nil, err
	}

	return &Address{
		Address: a,
	}, nil
}
