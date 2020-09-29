package util

import (
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type CommonPrm struct {
	local bool
}

func (p *CommonPrm) WithLocalOnly(v bool) *CommonPrm {
	if p != nil {
		p.local = v
	}

	return p
}

func (p *CommonPrm) LocalOnly() bool {
	if p != nil {
		return p.local
	}

	return false
}

func CommonPrmFromV2(req interface {
	GetMetaHeader() *session.RequestMetaHeader
}) *CommonPrm {
	meta := req.GetMetaHeader()

	return &CommonPrm{
		local: meta.GetTTL() <= 1, // FIXME: use constant
	}
}
