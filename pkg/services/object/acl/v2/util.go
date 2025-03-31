package v2

import (
	"errors"
	"fmt"

	icrypto "github.com/nspcc-dev/neofs-node/internal/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

var errMissingContainerID = errors.New("missing container ID")

func getContainerIDFromRequest(req any) (cid.ID, error) {
	var mID *refs.ContainerID
	var id cid.ID

	switch v := req.(type) {
	case *protoobject.GetRequest:
		mID = v.GetBody().GetAddress().GetContainerId()
	case *protoobject.PutRequest:
		part, ok := v.GetBody().GetObjectPart().(*protoobject.PutRequest_Body_Init_)
		if !ok {
			return cid.ID{}, errors.New("can't get container ID in chunk")
		}
		if part == nil || part.Init == nil {
			return cid.ID{}, errors.New("nil oneof heading part")
		}
		mID = part.Init.GetHeader().GetContainerId()
	case *protoobject.HeadRequest:
		mID = v.GetBody().GetAddress().GetContainerId()
	case *protoobject.SearchRequest:
		mID = v.GetBody().GetContainerId()
	case *protoobject.SearchV2Request:
		mID = v.GetBody().GetContainerId()
	case *protoobject.DeleteRequest:
		mID = v.GetBody().GetAddress().GetContainerId()
	case *protoobject.GetRangeRequest:
		mID = v.GetBody().GetAddress().GetContainerId()
	case *protoobject.GetRangeHashRequest:
		mID = v.GetBody().GetAddress().GetContainerId()
	default:
		return cid.ID{}, errors.New("unknown request type")
	}

	if mID == nil {
		return cid.ID{}, errMissingContainerID
	}

	return id, id.FromProtoMessage(mID)
}

// originalBearerToken goes down to original request meta header and fetches
// bearer token from there.
func originalBearerToken(header *protosession.RequestMetaHeader) (*bearer.Token, error) {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	mt := header.GetBearerToken()
	if mt == nil {
		return nil, nil
	}

	var tok bearer.Token
	return &tok, tok.FromProtoMessage(mt)
}

// originalSessionToken goes down to original request meta header and fetches
// session token from there.
func originalSessionToken(header *protosession.RequestMetaHeader) (*sessionSDK.Object, error) {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	mt := header.GetSessionToken()
	if mt == nil {
		return nil, nil
	}

	var tok sessionSDK.Object
	err := tok.FromProtoMessage(mt)
	if err != nil {
		return nil, fmt.Errorf("invalid session token: %w", err)
	}

	return &tok, nil
}

// getObjectIDFromRequestBody decodes oid.ID from the common interface of the
// object reference's holders. Returns an error if object ID is missing in the request.
func getObjectIDFromRequestBody(body interface{ GetAddress() *refs.Address }) (*oid.ID, error) {
	mID := body.GetAddress().GetObjectId()
	if mID == nil {
		return nil, errors.New("missing object ID")
	}

	var id oid.ID
	err := id.FromProtoMessage(mID)
	if err != nil {
		return nil, err
	}

	return &id, nil
}

func ownerFromToken(token *sessionSDK.Object) (*user.ID, []byte, error) {
	if err := icrypto.AuthenticateToken(token); err != nil {
		return nil, nil, fmt.Errorf("authenticate session token: %w", err)
	}
	tokenIssuer := token.Issuer()
	return &tokenIssuer, token.IssuerPublicKeyBytes(), nil
}

func originalBodySignature(v *protosession.RequestVerificationHeader) *refs.Signature {
	if v == nil {
		return nil
	}

	for v.GetOrigin() != nil {
		v = v.GetOrigin()
	}

	return v.GetBodySignature()
}

// assertVerb checks that token verb corresponds to op.
func assertVerb(tok sessionSDK.Object, op acl.Op) bool {
	//nolint:exhaustive
	switch op {
	case acl.OpObjectPut:
		return tok.AssertVerb(sessionSDK.VerbObjectPut)
	case acl.OpObjectDelete:
		return tok.AssertVerb(sessionSDK.VerbObjectDelete)
	case acl.OpObjectGet:
		return tok.AssertVerb(sessionSDK.VerbObjectGet)
	case acl.OpObjectHead:
		return tok.AssertVerb(
			sessionSDK.VerbObjectHead,
			sessionSDK.VerbObjectGet,
			sessionSDK.VerbObjectDelete,
			sessionSDK.VerbObjectRange,
			sessionSDK.VerbObjectRangeHash)
	case acl.OpObjectSearch:
		return tok.AssertVerb(sessionSDK.VerbObjectSearch, sessionSDK.VerbObjectDelete)
	case acl.OpObjectRange:
		return tok.AssertVerb(sessionSDK.VerbObjectRange, sessionSDK.VerbObjectRangeHash)
	case acl.OpObjectHash:
		return tok.AssertVerb(sessionSDK.VerbObjectRangeHash)
	}

	return false
}

// assertSessionRelation checks if given token describing the NeoFS session
// relates to the given container and optional object. Missing object
// means that the context isn't bound to any NeoFS object in the container.
// Returns no error iff relation is correct. Criteria:
//
//	session is bound to the given container
//	object is not specified or session is bound to this object
//
// Session MUST be bound to the particular container, otherwise behavior is undefined.
func assertSessionRelation(tok sessionSDK.Object, cnr cid.ID, obj *oid.ID) error {
	if !tok.AssertContainer(cnr) {
		return errors.New("requested container is not related to the session")
	}

	if obj != nil && !tok.AssertObject(*obj) {
		return errors.New("requested object is not related to the session")
	}

	return nil
}
