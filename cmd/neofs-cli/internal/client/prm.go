package internal

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// here are small structures with public setters to share between parameter structures

type commonPrm struct {
	cli *client.Client
}

// SetClient sets the base client for NeoFS API communication.
func (x *commonPrm) SetClient(cli *client.Client) {
	x.cli = cli
}

type bearerTokenPrm struct {
	bearerToken *bearer.Token
}

// WithBearerToken sets the bearer token to be attached to the request.
func (x *bearerTokenPrm) WithBearerToken(tok bearer.Token) {
	x.bearerToken = &tok
}

type commonObjectPrm struct {
	commonPrm
	bearerTokenPrm
	signerPrm

	sessionToken *session.Object

	local bool

	xHeaders []string
}

// MarkLocal request as local-only.
func (x *commonObjectPrm) MarkLocal() {
	x.local = true
}

// WithXHeaders sets request X-Headers.
func (x *commonObjectPrm) WithXHeaders(hs ...string) {
	x.xHeaders = hs
}

// WithinSession sets the token of the session within which the request should be sent.
func (x *commonObjectPrm) WithinSession(tok session.Object) {
	x.sessionToken = &tok
}

type signerPrm struct {
	signer user.Signer
}

// SetPrivateKey sets ecdsa.PrivateKey to be used for the operation.
func (x *signerPrm) SetPrivateKey(key ecdsa.PrivateKey) {
	x.signer = user.NewAutoIDSigner(key)
}
