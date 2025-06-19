package v2

import (
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// RequestInfo groups parsed version-independent (from SDK library)
// request information and raw API request.
type RequestInfo struct {
	basicACL    acl.Basic
	requestRole acl.Role
	operation   acl.Op // put, get, head, etc.

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
