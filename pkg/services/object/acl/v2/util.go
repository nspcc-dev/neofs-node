package v2

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	refsV2 "github.com/nspcc-dev/neofs-api-go/v2/refs"
	sessionV2 "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

var errMissingContainerID = errors.New("missing container ID")

func getContainerIDFromRequest(req any) (cid.ID, error) {
	var idV2 *refsV2.ContainerID
	var id cid.ID

	switch v := req.(type) {
	case *objectV2.GetRequest:
		idV2 = v.GetBody().GetAddress().GetContainerID()
	case *objectV2.PutRequest:
		part, ok := v.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit)
		if !ok {
			return cid.ID{}, errors.New("can't get container ID in chunk")
		}

		idV2 = part.GetHeader().GetContainerID()
	case *objectV2.HeadRequest:
		idV2 = v.GetBody().GetAddress().GetContainerID()
	case *objectV2.SearchRequest:
		idV2 = v.GetBody().GetContainerID()
	case *objectV2.DeleteRequest:
		idV2 = v.GetBody().GetAddress().GetContainerID()
	case *objectV2.GetRangeRequest:
		idV2 = v.GetBody().GetAddress().GetContainerID()
	case *objectV2.GetRangeHashRequest:
		idV2 = v.GetBody().GetAddress().GetContainerID()
	default:
		return cid.ID{}, errors.New("unknown request type")
	}

	if idV2 == nil {
		return cid.ID{}, errMissingContainerID
	}

	return id, id.ReadFromV2(*idV2)
}

// originalBearerToken goes down to original request meta header and fetches
// bearer token from there.
func originalBearerToken(header *sessionV2.RequestMetaHeader) (*bearer.Token, error) {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	tokV2 := header.GetBearerToken()
	if tokV2 == nil {
		return nil, nil
	}

	var tok bearer.Token
	return &tok, tok.ReadFromV2(*tokV2)
}

// originalSessionToken goes down to original request meta header and fetches
// session token from there.
func originalSessionToken(header *sessionV2.RequestMetaHeader) (*sessionSDK.Object, error) {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	tokV2 := header.GetSessionToken()
	if tokV2 == nil {
		return nil, nil
	}

	var tok sessionSDK.Object

	err := tok.ReadFromV2(*tokV2)
	if err != nil {
		return nil, fmt.Errorf("invalid session token: %w", err)
	}

	return &tok, nil
}

// getObjectIDFromRequestBody decodes oid.ID from the common interface of the
// object reference's holders. Returns an error if object ID is missing in the request.
func getObjectIDFromRequestBody(body interface{ GetAddress() *refsV2.Address }) (*oid.ID, error) {
	idV2 := body.GetAddress().GetObjectID()
	if idV2 == nil {
		return nil, errors.New("missing object ID")
	}

	var id oid.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, err
	}

	return &id, nil
}

func ownerFromToken(token *sessionSDK.Object) (*user.ID, []byte, error) {
	// 1. First check signature of session token.
	if !token.VerifySignature() {
		return nil, nil, errInvalidSessionSig
	}

	// 2. Then check if session token owner issued the session token
	tokenIssuer := token.Issuer()
	key := token.IssuerPublicKeyBytes()

	if !isOwnerFromKey(tokenIssuer, key) {
		// TODO: #767 in this case we can issue all owner keys from neofs.id and check once again
		return nil, nil, errInvalidSessionOwner
	}

	return &tokenIssuer, key, nil
}

func originalBodySignature(v *sessionV2.RequestVerificationHeader) *refsV2.Signature {
	if v == nil {
		return nil
	}

	for v.GetOrigin() != nil {
		v = v.GetOrigin()
	}

	return v.GetBodySignature()
}

func isOwnerFromKey(id user.ID, key []byte) bool {
	if key == nil {
		return false
	}

	pubKey, err := keys.NewPublicKeyFromBytes(key, elliptic.P256())
	if err != nil {
		return false
	}

	return id.Equals(user.ResolveFromECDSAPublicKey(ecdsa.PublicKey(*pubKey)))
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
