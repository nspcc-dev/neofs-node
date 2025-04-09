package v2

import (
	"fmt"
)

const invalidRequestMessage = "malformed request"

func malformedRequestError(reason string) error {
	return fmt.Errorf("%s: %s", invalidRequestMessage, reason)
}

var (
	errEmptyBody   = malformedRequestError("empty body")
	errInvalidVerb = malformedRequestError("session token verb is invalid")
)
