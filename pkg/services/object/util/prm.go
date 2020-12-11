package util

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
)

type CommonPrm struct {
	local bool

	token *token.SessionToken

	bearer *token.BearerToken

	key *ecdsa.PrivateKey

	callOpts []client.CallOption
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

func (p *CommonPrm) WithBearerToken(token *token.BearerToken) *CommonPrm {
	if p != nil {
		p.bearer = token
	}

	return p
}

// WithPrivateKey sets private key to use during execution.
func (p *CommonPrm) WithPrivateKey(key *ecdsa.PrivateKey) *CommonPrm {
	if p != nil {
		p.key = key
	}

	return p
}

// PrivateKey returns private key to use during execution.
func (p *CommonPrm) PrivateKey() *ecdsa.PrivateKey {
	if p != nil {
		return p.key
	}

	return nil
}

// WithRemoteCallOptions sets call options remote remote client calls.
func (p *CommonPrm) WithRemoteCallOptions(opts ...client.CallOption) *CommonPrm {
	if p != nil {
		p.callOpts = opts
	}

	return p
}

// RemoteCallOptions return call options for remote client calls.
func (p *CommonPrm) RemoteCallOptions() []client.CallOption {
	if p != nil {
		return p.callOpts
	}

	return nil
}

func (p *CommonPrm) SessionToken() *token.SessionToken {
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

func CommonPrmFromV2(req interface {
	GetMetaHeader() *session.RequestMetaHeader
}) *CommonPrm {
	meta := req.GetMetaHeader()

	prm := &CommonPrm{
		local:    meta.GetTTL() <= 1, // FIXME: use constant
		token:    nil,
		bearer:   nil,
		callOpts: make([]client.CallOption, 0, 3),
	}

	prm.callOpts = append(prm.callOpts, client.WithTTL(meta.GetTTL()-1))

	if tok := meta.GetSessionToken(); tok != nil {
		prm.token = token.NewSessionTokenFromV2(tok)
		prm.callOpts = append(prm.callOpts, client.WithSession(prm.token))
	}

	if tok := meta.GetBearerToken(); tok != nil {
		prm.bearer = token.NewBearerTokenFromV2(tok)
		prm.callOpts = append(prm.callOpts, client.WithBearer(prm.bearer))
	}

	return prm
}
