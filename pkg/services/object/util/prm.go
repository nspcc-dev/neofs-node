package util

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type CommonPrm struct {
	local bool

	token *token.SessionToken
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

func (p *CommonPrm) WithSessionToken(token *token.SessionToken) *CommonPrm {
	if p != nil {
		p.token = token
	}

	return p
}

func (p *CommonPrm) SessionToken() *token.SessionToken {
	if p != nil {
		return p.token
	}

	return nil
}

func CommonPrmFromV2(req interface {
	GetMetaHeader() *session.RequestMetaHeader
}) *CommonPrm {
	meta := req.GetMetaHeader()

	return &CommonPrm{
		local: meta.GetTTL() <= 1, // FIXME: use constant
		token: token.NewSessionTokenFromV2(meta.GetSessionToken()),
	}
}
