package headsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

type Prm struct {
	common *util.CommonPrm

	short bool

	addr *object.Address

	raw bool
}

func (p *Prm) WithCommonPrm(v *util.CommonPrm) *Prm {
	if p != nil {
		p.common = v
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

func (p *Prm) WithRaw(v bool) *Prm {
	if p != nil {
		p.raw = v
	}

	return p
}
