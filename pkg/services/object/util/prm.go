package util

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/object/common"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
)

// maxLocalTTL is maximum TTL for an operation to be considered local.
const maxLocalTTL = 1

type CommonPrm struct {
	local bool

	tokens common.RequestTokens

	ttl uint32

	xhdrs []string
}

// TTL returns TTL for new requests.
func (p *CommonPrm) TTL() uint32 {
	if p != nil {
		return p.ttl
	}

	return 1
}

// XHeaders returns X-Headers for new requests.
func (p *CommonPrm) XHeaders() []string {
	if p != nil {
		return p.xhdrs
	}

	return nil
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

func (p *CommonPrm) SessionToken() *session.Object {
	if p != nil {
		return p.tokens.SessionV1
	}

	return nil
}

func (p *CommonPrm) SessionTokenV2() *sessionv2.Token {
	if p != nil {
		return p.tokens.Session
	}

	return nil
}

func (p *CommonPrm) BearerToken() *bearer.Token {
	if p != nil {
		return p.tokens.Bearer
	}

	return nil
}

// ForgetTokens forgets all the tokens read from the request's
// meta information before.
func (p *CommonPrm) ForgetTokens() {
	if p != nil {
		p.tokens = common.RequestTokens{}
	}
}

// CommonPrmFromRequest is a temporary copy-paste of [CommonPrmFromV2].
func CommonPrmFromRequest(ttl uint32, xHdrs []*protosession.XHeader, tokens common.RequestTokens) *CommonPrm {
	prm := &CommonPrm{
		local:  ttl <= maxLocalTTL,
		tokens: tokens,
		ttl:    ttl - 1, // decrease TTL for new requests
		xhdrs:  make([]string, 0, 2*len(xHdrs)),
	}
	for i := range xHdrs {
		prm.xhdrs = append(prm.xhdrs, xHdrs[i].GetKey(), xHdrs[i].GetValue())
	}
	return prm
}
