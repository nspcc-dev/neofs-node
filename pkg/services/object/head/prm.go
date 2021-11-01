package headsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

type Prm struct {
	addr *object.Address
}

func (p *Prm) WithAddress(v *object.Address) *Prm {
	if p != nil {
		p.addr = v
	}

	return p
}
