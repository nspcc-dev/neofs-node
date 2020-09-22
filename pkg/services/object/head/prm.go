package headsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

type Prm struct {
	local, short bool

	addr *object.Address
}

func (p *Prm) OnlyLocal(v bool) *Prm {
	if p != nil {
		p.local = v
	}

	return p
}

func (p *Prm) Short(v bool) *Prm {
	if p != nil {
		p.short = v
	}

	return p
}

func (p *Prm) WithAddress(v *object.Address) *Prm {
	if p != nil {
		p.addr = v
	}

	return p
}
