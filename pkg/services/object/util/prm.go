package util

import (
	"crypto/ecdsa"
	"strconv"

	"github.com/nspcc-dev/neofs-api-go/pkg"
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	sessionsdk "github.com/nspcc-dev/neofs-api-go/pkg/session"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type CommonPrm struct {
	local bool

	netmapEpoch, netmapLookupDepth uint64

	token *sessionsdk.Token

	bearer *token.BearerToken

	callOpts []client.CallOption
}

type remoteCallOpts struct {
	opts []client.CallOption
}

type DynamicCallOption func(*remoteCallOpts)

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

func (p *CommonPrm) WithSessionToken(token *sessionsdk.Token) *CommonPrm {
	if p != nil {
		p.token = token
	}

	return p
}

func (p *CommonPrm) WithBearerToken(token *token.BearerToken) *CommonPrm {
	if p != nil {
		p.bearer = token
	}

	return p
}

// WithRemoteCallOptions sets call options remote remote client calls.
func (p *CommonPrm) WithRemoteCallOptions(opts ...client.CallOption) *CommonPrm {
	if p != nil {
		p.callOpts = opts
	}

	return p
}

// RemoteCallOptions return call options for remote client calls.
func (p *CommonPrm) RemoteCallOptions(dynamic ...DynamicCallOption) []client.CallOption {
	if p != nil {
		o := &remoteCallOpts{
			opts: p.callOpts,
		}

		for _, applier := range dynamic {
			applier(o)
		}

		return o.opts
	}

	return nil
}

func WithNetmapEpoch(v uint64) DynamicCallOption {
	return func(o *remoteCallOpts) {
		xHdr := pkg.NewXHeader()
		xHdr.SetKey(session.XHeaderNetmapEpoch)
		xHdr.SetValue(strconv.FormatUint(v, 10))

		o.opts = append(o.opts, client.WithXHeader(xHdr))
	}
}

// WithKey sets key to use for the request.
func WithKey(key *ecdsa.PrivateKey) DynamicCallOption {
	return func(o *remoteCallOpts) {
		o.opts = append(o.opts, client.WithKey(key))
	}
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

	const staticOptNum = 3

	prm := &CommonPrm{
		local:    meta.GetTTL() <= 1, // FIXME: use constant
		token:    nil,
		bearer:   nil,
		callOpts: make([]client.CallOption, 0, staticOptNum+len(xHdrs)),
	}

	prm.callOpts = append(prm.callOpts, client.WithTTL(meta.GetTTL()-1))

	if tok := meta.GetSessionToken(); tok != nil {
		prm.token = sessionsdk.NewTokenFromV2(tok)
		prm.callOpts = append(prm.callOpts, client.WithSession(prm.token))
	}

	if tok := meta.GetBearerToken(); tok != nil {
		prm.bearer = token.NewBearerTokenFromV2(tok)
		prm.callOpts = append(prm.callOpts, client.WithBearer(prm.bearer))
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
			prm.callOpts = append(prm.callOpts,
				client.WithXHeader(
					pkg.NewXHeaderFromV2(xHdrs[i]),
				),
			)
		}
	}

	return prm, nil
}
