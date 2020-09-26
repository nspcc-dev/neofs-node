package rangesvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

type Prm struct {
	local, full bool

	addr *object.Address

	rng *object.Range

	traverser *rangeTraverser
}

func (p *Prm) OnlyLocal(v bool) *Prm {
	if p != nil {
		p.local = v
	}

	return p
}

func (p *Prm) WithAddress(v *object.Address) *Prm {
	if p != nil {
		p.addr = v
	}

	return p
}

func (p *Prm) WithRange(v *object.Range) *Prm {
	if p != nil {
		p.rng = v
	}

	return p
}

func (p *Prm) FullRange() *Prm {
	if p != nil {
		p.full = true
	}

	return p
}
