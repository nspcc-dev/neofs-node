package searchsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
)

type Prm struct {
	local bool

	cid *container.ID

	query query.Query
}

func (p *Prm) WithContainerID(v *container.ID) *Prm {
	if p != nil {
		p.cid = v
	}

	return p
}

func (p *Prm) WithSearchQuery(v query.Query) *Prm {
	if p != nil {
		p.query = v
	}

	return p
}

func (p *Prm) OnlyLocal(v bool) *Prm {
	if p != nil {
		p.local = v
	}

	return p
}
