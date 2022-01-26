package internal

import (
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/token"
)

// here are small structures with public setters to share between parameter structures

type commonPrm struct {
	cli *client.Client
}

// SetClient sets base client for NeoFS API communication.
func (x *commonPrm) SetClient(cli *client.Client) {
	x.cli = cli
}

type containerIDPrm struct {
	cnrID *cid.ID
}

// SetContainerID sets container identifier.
func (x *containerIDPrm) SetContainerID(id *cid.ID) {
	x.cnrID = id
}

type sessionTokenPrm struct {
	sessionToken *session.Token
}

// SetSessionToken sets token of the session within which request should be sent.
func (x *sessionTokenPrm) SetSessionToken(tok *session.Token) {
	x.sessionToken = tok
}

type bearerTokenPrm struct {
	bearerToken *token.BearerToken
}

// SetBearerToken sets bearer token to be attached to the request.
func (x *bearerTokenPrm) SetBearerToken(tok *token.BearerToken) {
	x.bearerToken = tok
}

type objectAddressPrm struct {
	objAddr *addressSDK.Address
}

func (x *objectAddressPrm) SetAddress(addr *addressSDK.Address) {
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

// SetPayloadWriter sets writer of the object payload.
func (x *payloadWriterPrm) SetPayloadWriter(wrt io.Writer) {
	x.wrt = wrt
}

type commonObjectPrm struct {
	commonPrm
	sessionTokenPrm
	bearerTokenPrm

	opts []client.CallOption
}

// SetTTL sets request TTL value.
func (x *commonObjectPrm) SetTTL(ttl uint32) {
	x.opts = append(x.opts, client.WithTTL(ttl))
}

// SetXHeaders sets request X-Headers.
func (x *commonObjectPrm) SetXHeaders(xhdrs []*session.XHeader) {
	for _, xhdr := range xhdrs {
		x.opts = append(x.opts, client.WithXHeader(xhdr))
	}
}
