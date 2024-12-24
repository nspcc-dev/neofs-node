package util

import (
	"fmt"

	apiacl "github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	protosession "github.com/nspcc-dev/neofs-api-go/v2/session/grpc"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
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
	if mt := meta.GetSessionToken(); mt != nil {
		var st2 session.Token
		if err := st2.FromGRPCMessage(mt); err != nil {
			panic(err)
		}
		st = new(sessionsdk.Object)
		if err := st.ReadFromV2(st2); err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}
	}

	var bt *bearer.Token
	if mt := meta.GetBearerToken(); mt != nil {
		var bt2 apiacl.BearerToken
		if err := bt2.FromGRPCMessage(mt); err != nil {
			panic(err)
		}
		bt = new(bearer.Token)
		if err := bt.ReadFromV2(bt2); err != nil {
			return nil, fmt.Errorf("invalid bearer token: %w", err)
		}
	}

	xHdrs := meta.GetXHeaders()
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
