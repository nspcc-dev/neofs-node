package rangesvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

type Prm struct {
	common *util.CommonPrm

	full bool

	addr *object.Address

	rng *object.Range
}

func (p *Prm) WithCommonPrm(v *util.CommonPrm) *Prm {
	if p != nil {
		p.common = v
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
