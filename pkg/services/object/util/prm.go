package util

import (
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionsdk "github.com/nspcc-dev/neofs-sdk-go/session"
)

// maxLocalTTL is maximum TTL for an operation to be considered local.
const maxLocalTTL = 1

type CommonPrm struct {
	local bool

	token *sessionsdk.Object

	bearer *bearer.Token

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

func (p *CommonPrm) SessionToken() *sessionsdk.Object {
	if p != nil {
		return p.token
	}

	return nil
}

func (p *CommonPrm) BearerToken() *bearer.Token {
	if p != nil {
		return p.bearer
	}

	return nil
}

// ForgetTokens forgets all the tokens read from the request's
// meta information before.
func (p *CommonPrm) ForgetTokens() {
	if p != nil {
		p.token = nil
		p.bearer = nil
	}
}

// CommonPrmFromRequest is a temporary copy-paste of [CommonPrmFromV2].
func CommonPrmFromRequest(req interface {
	GetMetaHeader() *protosession.RequestMetaHeader
}) (*CommonPrm, error) {
	meta := req.GetMetaHeader()
	ttl := meta.GetTtl()

	// unwrap meta header to get original request meta information
	for meta.GetOrigin() != nil {
		meta = meta.GetOrigin()
	}

	var st *sessionsdk.Object
	if meta.SessionToken != nil {
		st = new(sessionsdk.Object)
		if err := st.FromProtoMessage(meta.SessionToken); err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}
	}

	var bt *bearer.Token
	if meta.BearerToken != nil {
		bt = new(bearer.Token)
		if err := bt.FromProtoMessage(meta.BearerToken); err != nil {
			return nil, fmt.Errorf("invalid bearer token: %w", err)
		}
	}

	xHdrs := meta.XHeaders
	prm := &CommonPrm{
		local:  ttl <= maxLocalTTL,
		token:  st,
		bearer: bt,
		ttl:    ttl - 1, // decrease TTL for new requests
		xhdrs:  make([]string, 0, 2*len(xHdrs)),
	}
	for i := range xHdrs {
		prm.xhdrs = append(prm.xhdrs, xHdrs[i].GetKey(), xHdrs[i].GetValue())
	}
	return prm, nil
}
