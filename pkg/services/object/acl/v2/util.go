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
	containerIDSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

var errMissingContainerID = errors.New("missing container ID")

func getContainerIDFromRequest(req interface{}) (containerIDSDK.ID, error) {
	var idV2 *refsV2.ContainerID
	var id containerIDSDK.ID

	switch v := req.(type) {
	case *objectV2.GetRequest:
		idV2 = v.GetBody().GetAddress().GetContainerID()
	case *objectV2.PutRequest:
		part, ok := v.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit)
		if !ok {
			return containerIDSDK.ID{}, errors.New("can't get container ID in chunk")
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
		return containerIDSDK.ID{}, errors.New("unknown request type")
	}

	if idV2 == nil {
		return containerIDSDK.ID{}, errMissingContainerID
	}

	return id, id.ReadFromV2(*idV2)
}

// originalBearerToken goes down to original request meta header and fetches
// bearer token from there.
func originalBearerToken(header *sessionV2.RequestMetaHeader) *bearer.Token {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	tokV2 := header.GetBearerToken()
	if tokV2 == nil {
		return nil
	}

	var tok bearer.Token
	tok.ReadFromV2(*tokV2)

	return &tok
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

func getObjectIDFromRequestBody(body interface{}) (*oidSDK.ID, error) {
	var idV2 *refsV2.ObjectID

	switch v := body.(type) {
	default:
		return nil, nil
	case interface {
		GetObjectID() *refsV2.ObjectID
	}:
		idV2 = v.GetObjectID()
	case interface {
		GetAddress() *refsV2.Address
	}:
		idV2 = v.GetAddress().GetObjectID()
	}

	if idV2 == nil {
		return nil, nil
	}

	var id oidSDK.ID

	err := id.ReadFromV2(*idV2)
	if err != nil {
		return nil, err
	}

	return &id, nil
}

func useObjectIDFromSession(req *RequestInfo, token *sessionSDK.Object) {
	if token == nil {
		return
	}

	// TODO(@cthulhu-rider): It'd be nice to not pull object identifiers from
	//  the token, but assert them. Track #1420
	var tokV2 sessionV2.Token
	token.WriteToV2(&tokV2)

	ctx, ok := tokV2.GetBody().GetContext().(*sessionV2.ObjectSessionContext)
	if !ok {
		panic(fmt.Sprintf("wrong object session context %T, is it verified?", tokV2.GetBody().GetContext()))
	}

	idV2 := ctx.GetAddress().GetObjectID()
	if idV2 == nil {
		return
	}

	req.oid = new(oidSDK.ID)

	err := req.oid.ReadFromV2(*idV2)
	if err != nil {
		panic(fmt.Sprintf("unexpected protocol violation error after correct session token decoding: %v", err))
	}
}

func ownerFromToken(token *sessionSDK.Object) (*user.ID, *keys.PublicKey, error) {
	// 1. First check signature of session token.
	if !token.VerifySignature() {
		return nil, nil, fmt.Errorf("%w: invalid session token signature", ErrMalformedRequest)
	}

	// 2. Then check if session token owner issued the session token
	// TODO(@cthulhu-rider): #1387 implement and use another approach to avoid conversion
	var tokV2 sessionV2.Token
	token.WriteToV2(&tokV2)

	ownerSessionV2 := tokV2.GetBody().GetOwnerID()
	if ownerSessionV2 == nil {
		return nil, nil, errors.New("missing session owner")
	}

	var ownerSession user.ID

	err := ownerSession.ReadFromV2(*ownerSessionV2)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid session token: %w", err)
	}

	tokenIssuerKey, err := unmarshalPublicKey(tokV2.GetSignature().GetKey())
	if err != nil {
		return nil, nil, fmt.Errorf("invalid key in session token signature: %w", err)
	}

	if !isOwnerFromKey(&ownerSession, tokenIssuerKey) {
		// TODO: #767 in this case we can issue all owner keys from neofs.id and check once again
		return nil, nil, fmt.Errorf("%w: invalid session token owner", ErrMalformedRequest)
	}

	return &ownerSession, tokenIssuerKey, nil
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

func unmarshalPublicKey(bs []byte) (*keys.PublicKey, error) {
	return keys.NewPublicKeyFromBytes(bs, elliptic.P256())
}

func isOwnerFromKey(id *user.ID, key *keys.PublicKey) bool {
	if id == nil || key == nil {
		return false
	}

	var id2 user.ID
	user.IDFromKey(&id2, (ecdsa.PublicKey)(*key))

	return id2.Equals(*id)
}

// assertVerb checks that token verb corresponds to op.
func assertVerb(tok sessionSDK.Object, op eaclSDK.Operation) bool {
	//nolint:exhaustive
	switch op {
	case eaclSDK.OperationPut:
		return tok.AssertVerb(sessionSDK.VerbObjectPut, sessionSDK.VerbObjectDelete)
	case eaclSDK.OperationDelete:
		return tok.AssertVerb(sessionSDK.VerbObjectDelete)
	case eaclSDK.OperationGet:
		return tok.AssertVerb(sessionSDK.VerbObjectGet)
	case eaclSDK.OperationHead:
		return tok.AssertVerb(
			sessionSDK.VerbObjectHead,
			sessionSDK.VerbObjectGet,
			sessionSDK.VerbObjectDelete,
			sessionSDK.VerbObjectRange,
			sessionSDK.VerbObjectRangeHash)
	case eaclSDK.OperationSearch:
		return tok.AssertVerb(sessionSDK.VerbObjectSearch, sessionSDK.VerbObjectDelete)
	case eaclSDK.OperationRange:
		return tok.AssertVerb(sessionSDK.VerbObjectRange, sessionSDK.VerbObjectRangeHash)
	case eaclSDK.OperationRangeHash:
		return tok.AssertVerb(sessionSDK.VerbObjectRangeHash)
	}

	return false
}
