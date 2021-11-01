package util

import (
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	sessionsdk "github.com/nspcc-dev/neofs-api-go/pkg/session"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type CommonPrm struct {
	local bool

	netmapEpoch, netmapLookupDepth uint64

	token *sessionsdk.Token

	bearer *token.BearerToken

	ttl uint32

	xhdrs []*pkg.XHeader
}

// TTL returns TTL for new requests.
func (p *CommonPrm) TTL() uint32 {
	if p != nil {
		return p.ttl
	}

	return 0
}

// Returns X-Headers for new requests.
func (p *CommonPrm) XHeaders() []*pkg.XHeader {
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

func (p *CommonPrm) SessionToken() *sessionsdk.Token {
	if p != nil {
		return p.token
	}

	return nil
}

func (p *CommonPrm) BearerToken() *token.BearerToken {
	if p != nil {
		return p.bearer
	}

	return nil
}

func (p *CommonPrm) NetmapEpoch() uint64 {
	if p != nil {
		return p.netmapEpoch
	}

	return 0
}

func (p *CommonPrm) NetmapLookupDepth() uint64 {
	if p != nil {
		return p.netmapLookupDepth
	}

	return 0
}

func (p *CommonPrm) SetNetmapLookupDepth(v uint64) {
	if p != nil {
		p.netmapLookupDepth = v
	}
}

func CommonPrmFromV2(req interface {
	GetMetaHeader() *session.RequestMetaHeader
}) (*CommonPrm, error) {
	meta := req.GetMetaHeader()

	xHdrs := meta.GetXHeaders()
	ttl := meta.GetTTL()

	prm := &CommonPrm{
		local: ttl <= 1, // FIXME: use constant
		xhdrs: make([]*pkg.XHeader, 0, len(xHdrs)),
		ttl:   ttl - 1, // decrease TTL for new requests
	}

	if tok := meta.GetSessionToken(); tok != nil {
		prm.token = sessionsdk.NewTokenFromV2(tok)
	}

	if tok := meta.GetBearerToken(); tok != nil {
		prm.bearer = token.NewBearerTokenFromV2(tok)
	}

	for i := range xHdrs {
		switch xHdrs[i].GetKey() {
		case session.XHeaderNetmapEpoch:
			var err error

			prm.netmapEpoch, err = strconv.ParseUint(xHdrs[i].GetValue(), 10, 64)
			if err != nil {
				return nil, err
			}
		case session.XHeaderNetmapLookupDepth:
			var err error

			prm.netmapLookupDepth, err = strconv.ParseUint(xHdrs[i].GetValue(), 10, 64)
			if err != nil {
				return nil, err
			}
		default:
			xhdr := pkg.NewXHeaderFromV2(xHdrs[i])

			prm.xhdrs = append(prm.xhdrs, xhdr)
		}
	}

	return prm, nil
}
