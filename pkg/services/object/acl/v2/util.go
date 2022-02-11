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
	containerIDSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	eaclSDK "github.com/nspcc-dev/neofs-sdk-go/eacl"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	sessionSDK "github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/signature"
	bearerSDK "github.com/nspcc-dev/neofs-sdk-go/token"
)

func getContainerIDFromRequest(req interface{}) (id *containerIDSDK.ID, err error) {
	switch v := req.(type) {
	case *objectV2.GetRequest:
		return containerIDSDK.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *objectV2.PutRequest:
		objPart := v.GetBody().GetObjectPart()
		if part, ok := objPart.(*objectV2.PutObjectPartInit); ok {
			return containerIDSDK.NewFromV2(part.GetHeader().GetContainerID()), nil
		}

		return nil, errors.New("can't get container ID in chunk")
	case *objectV2.HeadRequest:
		return containerIDSDK.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *objectV2.SearchRequest:
		return containerIDSDK.NewFromV2(v.GetBody().GetContainerID()), nil
	case *objectV2.DeleteRequest:
		return containerIDSDK.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *objectV2.GetRangeRequest:
		return containerIDSDK.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	case *objectV2.GetRangeHashRequest:
		return containerIDSDK.NewFromV2(v.GetBody().GetAddress().GetContainerID()), nil
	default:
		return nil, errors.New("unknown request type")
	}
}

// originalBearerToken goes down to original request meta header and fetches
// bearer token from there.
func originalBearerToken(header *sessionV2.RequestMetaHeader) *bearerSDK.BearerToken {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	return bearerSDK.NewBearerTokenFromV2(header.GetBearerToken())
}

// originalSessionToken goes down to original request meta header and fetches
// session token from there.
func originalSessionToken(header *sessionV2.RequestMetaHeader) *sessionSDK.Token {
	for header.GetOrigin() != nil {
		header = header.GetOrigin()
	}

	return sessionSDK.NewTokenFromV2(header.GetSessionToken())
}

func getObjectIDFromRequestBody(body interface{}) *oidSDK.ID {
	switch v := body.(type) {
	default:
		return nil
	case interface {
		GetObjectID() *refsV2.ObjectID
	}:
		return oidSDK.NewIDFromV2(v.GetObjectID())
	case interface {
		GetAddress() *refsV2.Address
	}:
		return oidSDK.NewIDFromV2(v.GetAddress().GetObjectID())
	}
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
// returns reqVerb.
func sourceVerbOfRequest(req MetaWithToken, reqVerb eaclSDK.Operation) eaclSDK.Operation {
	if req.token != nil {
		switch v := req.token.Context().(type) {
		case *sessionSDK.ObjectContext:
			return tokenVerbToOperation(v)
		default:
			// do nothing, return request verb
		}
	}

	return reqVerb
}

func useObjectIDFromSession(req *RequestInfo, token *sessionSDK.Token) {
	if token == nil {
		return
	}

	objCtx, ok := token.Context().(*sessionSDK.ObjectContext)
	if !ok {
		return
	}

	req.oid = objCtx.Address().ObjectID()
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
	tokenIssuerKey := unmarshalPublicKey(token.Signature().Key())
	tokenOwner := token.OwnerID()

	if !isOwnerFromKey(tokenOwner, tokenIssuerKey) {
		// TODO: #767 in this case we can issue all owner keys from neofs.id and check once again
		return nil, nil, fmt.Errorf("%w: invalid session token owner", ErrMalformedRequest)
	}

	return tokenOwner, tokenIssuerKey, nil
}

func originalBodySignature(v *sessionV2.RequestVerificationHeader) *signature.Signature {
	if v == nil {
		return nil
	}

	for v.GetOrigin() != nil {
		v = v.GetOrigin()
	}

	return signature.NewFromV2(v.GetBodySignature())
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
