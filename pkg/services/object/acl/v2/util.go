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
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
)

var errMissingContainerID = errors.New("missing container ID")

func getContainerIDFromRequest(req interface{}) (*containerIDSDK.ID, error) {
	var idV2 *refsV2.ContainerID
	id := new(containerIDSDK.ID)

	switch v := req.(type) {
	case *objectV2.GetRequest:
		idV2 = v.GetBody().GetAddress().GetContainerID()
	case *objectV2.PutRequest:
		part, ok := v.GetBody().GetObjectPart().(*objectV2.PutObjectPartInit)
		if !ok {
			return nil, errors.New("can't get container ID in chunk")
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
		return nil, errors.New("unknown request type")
	}

	if idV2 == nil {
		return nil, errMissingContainerID
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
func originalSessionToken(header *sessionV2.RequestMetaHeader) *sessionSDK.Token {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	return sessionSDK.NewTokenFromV2(header.GetSessionToken())
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

func getObjectOwnerFromMessage(req interface{}) (id *owner.ID, err error) {
	switch v := req.(type) {
	case *objectV2.PutRequest:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*objectV2.PutObjectPartInit); ok {
			return owner.NewIDFromV2(part.GetHeader().GetOwnerID()), nil
		}

		return nil, errors.New("can't get container ID in chunk")
	case *objectV2.GetResponse:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*objectV2.GetObjectPartInit); ok {
			return owner.NewIDFromV2(part.GetHeader().GetOwnerID()), nil
		}

		return nil, errors.New("can't get container ID in chunk")
	default:
		return nil, errors.New("unsupported request type")
	}
}

// sourceVerbOfRequest looks for verb in session token and if it is not found,
// returns reqVerb. Second return value is true if operation is unknown.
func sourceVerbOfRequest(tok *sessionSDK.Token, reqVerb eaclSDK.Operation) (eaclSDK.Operation, bool) {
	ctx, ok := tok.Context().(*sessionSDK.ObjectContext)
	if ok {
		op := tokenVerbToOperation(ctx)
		if op != eaclSDK.OperationUnknown {
			return op, false
		}
	}

	return reqVerb, true
}

func useObjectIDFromSession(req *RequestInfo, token *sessionSDK.Token) {
	if token == nil {
		return
	}

	objCtx, ok := token.Context().(*sessionSDK.ObjectContext)
	if !ok {
		return
	}

	id, ok := objCtx.Address().ObjectID()
	if ok {
		req.oid = &id
	}
}

func tokenVerbToOperation(ctx *sessionSDK.ObjectContext) eaclSDK.Operation {
	switch {
	case ctx.IsForGet():
		return eaclSDK.OperationGet
	case ctx.IsForPut():
		return eaclSDK.OperationPut
	case ctx.IsForHead():
		return eaclSDK.OperationHead
	case ctx.IsForSearch():
		return eaclSDK.OperationSearch
	case ctx.IsForDelete():
		return eaclSDK.OperationDelete
	case ctx.IsForRange():
		return eaclSDK.OperationRange
	case ctx.IsForRangeHash():
		return eaclSDK.OperationRangeHash
	default:
		return eaclSDK.OperationUnknown
	}
}

func ownerFromToken(token *sessionSDK.Token) (*owner.ID, *keys.PublicKey, error) {
	// 1. First check signature of session token.
	if !token.VerifySignature() {
		return nil, nil, fmt.Errorf("%w: invalid session token signature", ErrMalformedRequest)
	}

	// 2. Then check if session token owner issued the session token
	// TODO(@cthulhu-rider): #1387 implement and use another approach to avoid conversion
	tokV2 := token.ToV2()

	tokenIssuerKey := unmarshalPublicKey(tokV2.GetSignature().GetKey())
	tokenOwner := token.OwnerID()

	if !isOwnerFromKey(tokenOwner, tokenIssuerKey) {
		// TODO: #767 in this case we can issue all owner keys from neofs.id and check once again
		return nil, nil, fmt.Errorf("%w: invalid session token owner", ErrMalformedRequest)
	}

	return tokenOwner, tokenIssuerKey, nil
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

func unmarshalPublicKey(bs []byte) *keys.PublicKey {
	pub, err := keys.NewPublicKeyFromBytes(bs, elliptic.P256())
	if err != nil {
		return nil
	}
	return pub
}

func isOwnerFromKey(id *owner.ID, key *keys.PublicKey) bool {
	if id == nil || key == nil {
		return false
	}

	return id.Equal(owner.NewIDFromPublicKey((*ecdsa.PublicKey)(key)))
}

// isVerbCompatible checks that tokenVerb operation can create auxiliary op operation.
func isVerbCompatible(tokenVerb, op eaclSDK.Operation) bool {
	switch tokenVerb {
	case eaclSDK.OperationGet:
		return op == eaclSDK.OperationGet || op == eaclSDK.OperationHead
	case eaclSDK.OperationDelete:
		return op == eaclSDK.OperationPut || op == eaclSDK.OperationHead ||
			op == eaclSDK.OperationSearch
	case eaclSDK.OperationRange, eaclSDK.OperationRangeHash:
		return op == eaclSDK.OperationRange || op == eaclSDK.OperationHead
	default:
		return tokenVerb == op
	}
}
