package util

import (
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	sessionsdk "github.com/nspcc-dev/neofs-sdk-go/session"
)

// maxLocalTTL is maximum TTL for an operation to be considered local.
const maxLocalTTL = 1

type CommonPrm struct {
	local bool

	netmapEpoch, netmapLookupDepth uint64

	token *sessionsdk.Token

	bearer *bearer.Token

	ttl uint32

	xhdrs []*sessionsdk.XHeader
}

// TTL returns TTL for new requests.
func (p *CommonPrm) TTL() uint32 {
	if p != nil {
		return p.ttl
	}

	return 1
}

// XHeaders returns X-Headers for new requests.
func (p *CommonPrm) XHeaders() []*sessionsdk.XHeader {
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

func (p *CommonPrm) BearerToken() *bearer.Token {
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
		local: ttl <= maxLocalTTL,
		xhdrs: make([]*sessionsdk.XHeader, 0, len(xHdrs)),
		ttl:   ttl - 1, // decrease TTL for new requests
	}

	if tok := meta.GetSessionToken(); tok != nil {
		prm.token = sessionsdk.NewTokenFromV2(tok)
	}

	if tok := meta.GetBearerToken(); tok != nil {
		prm.bearer = new(bearer.Token)
		prm.bearer.ReadFromV2(*tok)
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
			xhdr := sessionsdk.NewXHeaderFromV2(&xHdrs[i])

			prm.xhdrs = append(prm.xhdrs, xhdr)
		}
	}

	return prm, nil
}
