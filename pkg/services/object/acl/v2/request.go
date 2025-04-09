package v2

import (
	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// RequestInfo groups parsed version-independent (from SDK library)
// request information and raw API request.
type RequestInfo struct {
	basicACL    acl.Basic
	requestRole acl.Role
	operation   acl.Op  // put, get, head, etc.
	cnrOwner    user.ID // container owner

	idCnr cid.ID

	// optional for some request
	// e.g. Put, Search
	obj *oid.ID

	senderKey     []byte
	senderAccount *user.ID

	bearer *bearer.Token // bearer token of request

	srcRequest any
}

func (r *RequestInfo) SetBasicACL(basicACL acl.Basic) {
	r.basicACL = basicACL
}

func (r *RequestInfo) SetRequestRole(requestRole acl.Role) {
	r.requestRole = requestRole
}

func (r *RequestInfo) SetSenderKey(senderKey []byte) {
	r.senderKey = senderKey
}

// Request returns raw API request.
func (r RequestInfo) Request() any {
	return r.srcRequest
}

// ContainerOwner returns owner if the container.
func (r RequestInfo) ContainerOwner() user.ID {
	return r.cnrOwner
}

// ObjectID return object ID.
func (r RequestInfo) ObjectID() *oid.ID {
	return r.obj
}

// ContainerID return container ID.
func (r RequestInfo) ContainerID() cid.ID {
	return r.idCnr
}

// CleanBearer forces cleaning bearer token information.
func (r *RequestInfo) CleanBearer() {
	r.bearer = nil
}

// Bearer returns bearer token of the request.
func (r RequestInfo) Bearer() *bearer.Token {
	return r.bearer
}

// BasicACL returns basic ACL of the container.
func (r RequestInfo) BasicACL() acl.Basic {
	return r.basicACL
}

// SenderKey returns public key of the request's sender.
func (r RequestInfo) SenderKey() []byte {
	return r.senderKey
}

// SenderAccount returns account of the request's sender.
func (r RequestInfo) SenderAccount() *user.ID {
	return r.senderAccount
}

// Operation returns request's operation.
func (r RequestInfo) Operation() acl.Op {
	return r.operation
}

// RequestRole returns request sender's role.
func (r RequestInfo) RequestRole() acl.Role {
	return r.requestRole
}

// MetaWithToken groups session and bearer tokens,
// verification header and raw API request.
type MetaWithToken struct {
	vheader *protosession.RequestVerificationHeader
	token   *sessionSDK.Object
	bearer  *bearer.Token
	src     any
}

// RequestOwner returns ownerID and its public key
// according to internal meta information.
func (r MetaWithToken) RequestOwner() (*user.ID, []byte, error) {
	// if session token is presented, use it as truth source
	if r.token != nil {
		// verify signature of session token
		return ownerFromToken(r.token)
	}
	idSender, key, err := icrypto.GetRequestAuthor(r.vheader)
	if err != nil {
		return nil, nil, err
	}
	return &idSender, key, nil
}
