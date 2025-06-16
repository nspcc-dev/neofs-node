package v2

import (
	"errors"

	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"github.com/nspcc-dev/neofs-sdk-go/proto/refs"
	protosession "github.com/nspcc-dev/neofs-sdk-go/proto/session"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
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

// assertVerb checks that token is applicable to the particular request.
func assertVerb(tok sessionSDK.Object, reqVerb sessionSDK.ObjectVerb) bool {
	switch reqVerb {
	default:
		return tok.AssertVerb(reqVerb)
	case sessionSDK.VerbObjectHead:
		return tok.AssertVerb(
			sessionSDK.VerbObjectHead,
			sessionSDK.VerbObjectGet,
			sessionSDK.VerbObjectDelete,
			sessionSDK.VerbObjectRange,
			sessionSDK.VerbObjectRangeHash)
	case sessionSDK.VerbObjectSearch:
		return tok.AssertVerb(sessionSDK.VerbObjectSearch, sessionSDK.VerbObjectDelete)
	case sessionSDK.VerbObjectRange:
		return tok.AssertVerb(sessionSDK.VerbObjectRange, sessionSDK.VerbObjectRangeHash)
	}
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
func assertSessionRelation(tok sessionSDK.Object, cnr cid.ID, obj oid.ID) error {
	if !tok.AssertContainer(cnr) {
		return errors.New("requested container is not related to the session")
	}

	// if session relates to object's removal, we don't check
	// relation of the tombstone to the session here since user
	// can't predict tomb's ID.
	if !tok.AssertVerb(sessionSDK.VerbObjectDelete) && !obj.IsZero() && !tok.AssertObject(obj) {
		return errors.New("requested object is not related to the session")
	}

	return nil
}
