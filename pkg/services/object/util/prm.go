package util

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	sessionsdk "github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// maxLocalTTL is maximum TTL for an operation to be considered local.
const maxLocalTTL = 1

type CommonPrm struct {
	local bool

	netmapEpoch, netmapLookupDepth uint64

	token *sessionsdk.Object

	bearer *bearer.Token

	ttl uint32

	xhdrs []string

	ownerSession user.ID
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

func (p *CommonPrm) SessionOwner() (user.ID, bool) {
	if p != nil && p.token != nil {
		return p.ownerSession, true
	}

	return user.ID{}, false
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

	var tokenSession *sessionsdk.Object
	var err error
	var ownerSession user.ID

	if tokenSessionV2 := meta.GetSessionToken(); tokenSessionV2 != nil {
		ownerSessionV2 := tokenSessionV2.GetBody().GetOwnerID()
		if ownerSessionV2 == nil {
			return nil, errors.New("missing session owner")
		}

		err = ownerSession.ReadFromV2(*ownerSessionV2)
		if err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}

		tokenSession = new(sessionsdk.Object)

		err = tokenSession.ReadFromV2(*tokenSessionV2)
		if err != nil {
			return nil, fmt.Errorf("invalid session token: %w", err)
		}
	}

	xHdrs := meta.GetXHeaders()
	ttl := meta.GetTTL()

	prm := &CommonPrm{
		local:        ttl <= maxLocalTTL,
		token:        tokenSession,
		ttl:          ttl - 1, // decrease TTL for new requests
		xhdrs:        make([]string, 0, 2*len(xHdrs)),
		ownerSession: ownerSession,
	}

	if tok := meta.GetBearerToken(); tok != nil {
		prm.bearer = new(bearer.Token)
		prm.bearer.ReadFromV2(*tok)
	}

	for i := range xHdrs {
		switch key := xHdrs[i].GetKey(); key {
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
			prm.xhdrs = append(prm.xhdrs, key, xHdrs[i].GetValue())
		}
	}

	return prm, nil
}
