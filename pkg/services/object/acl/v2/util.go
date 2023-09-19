package v2acl

import (
	"fmt"

	v2session "github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/session"
)

// request is a common interface of all object requests used to implement common
// utilities.
type request interface {
	GetMetaHeader() *v2session.RequestMetaHeader
	GetVerificationHeader() *v2session.RequestVerificationHeader
}

// sessionTokenFromRequest returns session token attached to the request by
// original client. Returns both nil if the token is missing.
func sessionTokenFromRequest(req request) (*session.Object, error) {
	metaHeader := req.GetMetaHeader()
	for metaHeader.GetOrigin() != nil {
		metaHeader = metaHeader.GetOrigin()
	}

	if metaHeader == nil {
		return nil, nil
	}

	sessionTokenMsg := metaHeader.GetSessionToken()
	if sessionTokenMsg == nil {
		return nil, nil
	}

	var sessionToken session.Object

	err := sessionToken.ReadFromV2(*sessionTokenMsg)
	if err != nil {
		return nil, newInvalidRequestError(
			newInvalidMetaHeaderError(
				fmt.Errorf("invalid session token field: %w", err)))
	}

	return &sessionToken, nil
}

// bearerTokenFromRequest returns bearer token attached to the request by
// original client. Returns both nil if the token is missing.
func bearerTokenFromRequest(req request) (*bearer.Token, error) {
	metaHeader := req.GetMetaHeader()
	for metaHeader.GetOrigin() != nil {
		metaHeader = metaHeader.GetOrigin()
	}

	if metaHeader == nil {
		return nil, nil
	}

	bearerTokenMsg := metaHeader.GetBearerToken()
	if bearerTokenMsg == nil {
		return nil, nil
	}

	var bearerToken bearer.Token

	err := bearerToken.ReadFromV2(*bearerTokenMsg)
	if err != nil {
		return nil, newInvalidRequestError(
			newInvalidMetaHeaderError(
				fmt.Errorf("invalid bearer token field: %w", err)))
	}

	return &bearerToken, nil
}

// binaryClientPublicKeyFromRequest pulls binary-encoded public key used by
// original client to sign the request. If there is no error, the key is always
// non-empty.
func binaryClientPublicKeyFromRequest(req request) ([]byte, error) {
	verificationHeader := req.GetVerificationHeader()
	for verificationHeader.GetOrigin() != nil {
		verificationHeader = verificationHeader.GetOrigin()
	}

	if verificationHeader == nil {
		return nil, newInvalidRequestError(
			newInvalidVerificationHeaderError(errMissingRequestVerificationHeader))
	}

	sig := verificationHeader.GetBodySignature()
	if sig == nil {
		return nil, newInvalidRequestError(
			newInvalidVerificationHeaderError(errMissingBodySignature))
	}

	bSigKey := sig.GetKey()
	if len(bSigKey) == 0 {
		return nil, newInvalidRequestError(
			newInvalidVerificationHeaderError(
				fmt.Errorf("invalid body signature: %w", errMissingPublicKey)))
	}

	return bSigKey, nil
}

// requestWrapper is a wrapper over request providing needed interface.
type requestWrapper struct {
	xHeaders []v2session.XHeader
}

// wrapRequest wraps given request into requestWrapper.
func wrapRequest(r request) requestWrapper {
	metaHeader := r.GetMetaHeader()
	for metaHeader.GetOrigin() != nil {
		metaHeader = metaHeader.GetOrigin()
	}

	var w requestWrapper

	if metaHeader != nil {
		w.xHeaders = metaHeader.GetXHeaders()
	}

	return w
}

// GetRequestHeaderByKey looks up for X-header value set in the underlying
// request meta header by key. Returns zero if the X-header is missing.
func (x requestWrapper) GetRequestHeaderByKey(key string) string {
	for i := range x.xHeaders {
		if x.xHeaders[i].GetKey() == key {
			return x.xHeaders[i].GetValue()
		}
	}

	return ""
}
