package rangehashsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

type Prm struct {
	local bool

	addr *object.Address

	typ pkg.ChecksumType

	rngs []*object.Range

	salt []byte
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
