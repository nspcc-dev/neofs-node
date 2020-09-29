package rangehashsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

type Prm struct {
	common *util.CommonPrm

	addr *object.Address

	typ pkg.ChecksumType

	rngs []*object.Range

	salt []byte
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

func (p *Prm) WithChecksumType(typ pkg.ChecksumType) *Prm {
	if p != nil {
		p.typ = typ
	}

	return p
}

func (p *Prm) FromRanges(v ...*object.Range) *Prm {
	if p != nil {
		p.rngs = v
	}

	return p
}
