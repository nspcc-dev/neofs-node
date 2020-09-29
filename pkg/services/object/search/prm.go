package searchsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

type Prm struct {
	common *util.CommonPrm

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

func (p *Prm) WithCommonPrm(v *util.CommonPrm) *Prm {
	if p != nil {
		p.common = v
	}

	return p
}
