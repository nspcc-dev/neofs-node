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

// SetClient sets the base client for NeoFS API communication.
func (x *commonPrm) SetClient(cli *client.Client) {
	x.cli = cli
}

type containerIDPrm struct {
	cnrID *cid.ID
}

// SetContainerID sets the container identifier.
func (x *containerIDPrm) SetContainerID(id *cid.ID) {
	x.cnrID = id
}

type sessionTokenPrm struct {
	sessionToken *session.Token
}

// SetSessionToken sets the token of the session within which the request should be sent.
func (x *sessionTokenPrm) SetSessionToken(tok *session.Token) {
	x.sessionToken = tok
}

type bearerTokenPrm struct {
	bearerToken *token.BearerToken
}

// SetBearerToken sets the bearer token to be attached to the request.
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

// SetPayloadWriter sets the writer of the object payload.
func (x *payloadWriterPrm) SetPayloadWriter(wrt io.Writer) {
	x.wrt = wrt
}

type commonObjectPrm struct {
	commonPrm
	sessionTokenPrm
	bearerTokenPrm

	local bool

	xHeaders []*session.XHeader
}

// SetTTL sets request TTL value.
func (x *commonObjectPrm) SetTTL(ttl uint32) {
	x.local = ttl < 2
}

// SetXHeaders sets request X-Headers.
func (x *commonObjectPrm) SetXHeaders(hs []*session.XHeader) {
	x.xHeaders = hs
}

func (x commonObjectPrm) xHeadersPrm() (res []string) {
	if x.xHeaders != nil {
		res = make([]string, len(x.xHeaders)*2)

		for i := range x.xHeaders {
			res[2*i] = x.xHeaders[i].Key()
			res[2*i+1] = x.xHeaders[i].Value()
		}
	}

	return
}
