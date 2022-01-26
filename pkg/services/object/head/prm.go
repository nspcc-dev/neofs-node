package headsvc

import (
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

type Prm struct {
	addr *addressSDK.Address
}

func (p *Prm) WithAddress(v *addressSDK.Address) *Prm {
	if p != nil {
		p.addr = v
	}

	return p
}
