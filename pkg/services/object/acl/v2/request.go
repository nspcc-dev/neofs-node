package v2

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	sessionV2 "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	containerIDSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
)

// RequestInfo groups parsed version-independent (from SDK library)
// request information and raw API request.
type RequestInfo struct {
	basicACL    uint32
	requestRole eaclSDK.Role
	isInnerRing bool
	operation   eaclSDK.Operation // put, get, head, etc.
	cnrOwner    *owner.ID         // container owner

	idCnr *containerIDSDK.ID

	oid *oidSDK.ID

	senderKey []byte

	bearer *bearer.Token // bearer token of request

	srcRequest interface{}
}

func (r *RequestInfo) SetBasicACL(basicACL uint32) {
	r.basicACL = basicACL
}

func (r *RequestInfo) SetRequestRole(requestRole eaclSDK.Role) {
	r.requestRole = requestRole
}

func (r *RequestInfo) SetSenderKey(senderKey []byte) {
	r.senderKey = senderKey
}

// Request returns raw API request.
func (r RequestInfo) Request() interface{} {
	return r.srcRequest
}

// ContainerOwner returns owner if the container.
func (r RequestInfo) ContainerOwner() *owner.ID {
	return r.cnrOwner
}

// ObjectID return object ID.
func (r RequestInfo) ObjectID() *oidSDK.ID {
	return r.oid
}

// ContainerID return container ID.
func (r RequestInfo) ContainerID() *containerIDSDK.ID {
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

// IsInnerRing specifies if request was made by inner ring.
func (r RequestInfo) IsInnerRing() bool {
	return r.isInnerRing
}

// BasicACL returns basic ACL of the container.
func (r RequestInfo) BasicACL() uint32 {
	return r.basicACL
}

// SenderKey returns public key of the request's sender.
func (r RequestInfo) SenderKey() []byte {
	return r.senderKey
}

// Operation returns request's operation.
func (r RequestInfo) Operation() eaclSDK.Operation {
	return r.operation
}

// RequestRole returns request sender's role.
func (r RequestInfo) RequestRole() eaclSDK.Role {
	return r.requestRole
}

// MetaWithToken groups session and bearer tokens,
// verification header and raw API request.
type MetaWithToken struct {
	vheader *sessionV2.RequestVerificationHeader
	token   *sessionSDK.Token
	bearer  *bearer.Token
	src     interface{}
}

// RequestOwner returns ownerID and its public key
// according to internal meta information.
func (r MetaWithToken) RequestOwner() (*owner.ID, *keys.PublicKey, error) {
	if r.vheader == nil {
		return nil, nil, fmt.Errorf("%w: nil verification header", ErrMalformedRequest)
	}

	// if session token is presented, use it as truth source
	if r.token != nil {
		// verify signature of session token
		return ownerFromToken(r.token)
	}

	// otherwise get original body signature
	bodySignature := originalBodySignature(r.vheader)
	if bodySignature == nil {
		return nil, nil, fmt.Errorf("%w: nil at body signature", ErrMalformedRequest)
	}

	key := unmarshalPublicKey(bodySignature.GetKey())

	return owner.NewIDFromPublicKey((*ecdsa.PublicKey)(key)), key, nil
}
