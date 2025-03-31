package v2

import (
	"fmt"
)

const invalidRequestMessage = "malformed request"

func malformedRequestError(reason string) error {
	return fmt.Errorf("%s: %s", invalidRequestMessage, reason)
}

var (
	errEmptyBody               = malformedRequestError("empty body")
	errEmptyVerificationHeader = malformedRequestError("empty verification header")
	errEmptyBodySig            = malformedRequestError("empty at body signature")
	errInvalidSessionSig       = malformedRequestError("invalid session token signature")
	errInvalidVerb             = malformedRequestError("session token verb is invalid")
)
