package internal

import (
	"crypto/ecdsa"
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

type containerIDPrm struct {
	cnrID cid.ID
}

// SetContainerID sets the container identifier.
func (x *containerIDPrm) SetContainerID(id cid.ID) {
	x.cnrID = id
}

type bearerTokenPrm struct {
	bearerToken *bearer.Token
}

// SetBearerToken sets the bearer token to be attached to the request.
func (x *bearerTokenPrm) SetBearerToken(tok *bearer.Token) {
	x.bearerToken = tok
}

type objectAddressPrm struct {
	objAddr oid.Address
}

func (x *objectAddressPrm) SetAddress(addr oid.Address) {
	x.objAddr = addr
}

type rawPrm struct {
	raw bool
}

// SetRawFlag sets flag of raw request.
func (x *rawPrm) SetRawFlag(raw bool) {
	x.raw = raw
}

type payloadWriterPrm struct {
	wrt io.Writer
}

// SetPayloadWriter sets the writer of the object payload.
func (x *payloadWriterPrm) SetPayloadWriter(wrt io.Writer) {
	x.wrt = wrt
}

type commonObjectPrm struct {
	commonPrm
	bearerTokenPrm
	signerPrm

	sessionToken *session.Object

	local bool

	xHeaders []string
}

// SetTTL sets request TTL value.
func (x *commonObjectPrm) SetTTL(ttl uint32) {
	x.local = ttl < 2
}

// SetXHeaders sets request X-Headers.
func (x *commonObjectPrm) SetXHeaders(hs []string) {
	x.xHeaders = hs
}

// SetSessionToken sets the token of the session within which the request should be sent.
func (x *commonObjectPrm) SetSessionToken(tok *session.Object) {
	x.sessionToken = tok
}

type signerPrm struct {
	signer user.Signer
}

// SetPrivateKey sets ecdsa.PrivateKey to be used for the operation.
func (x *signerPrm) SetPrivateKey(key ecdsa.PrivateKey) {
	x.signer = user.NewAutoIDSigner(key)
}

type signerRFC6979Prm struct {
	signer user.Signer
}

// SetPrivateKey sets ecdsa.PrivateKey to be used for the operation.
func (p *signerRFC6979Prm) SetPrivateKey(key ecdsa.PrivateKey) {
	p.signer = user.NewAutoIDSignerRFC6979(key)
}
