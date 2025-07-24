package v2

import (
	"errors"
	"fmt"
)

const invalidRequestMessage = "malformed request"

func malformedRequestError(reason string) error {
	return fmt.Errorf("%s: %s", invalidRequestMessage, reason)
}

var (
	// ErrNotMatched is returned from CheckEACL() when there were no rules
	// found that match request/object. Most of the time this means there
	// were no object headers to check rules against, but it can also
	// mean the default behavior after the full table scan.
	ErrNotMatched = errors.New("no matching rule")

	errEmptyBody   = malformedRequestError("empty body")
	errInvalidVerb = malformedRequestError("session token verb is invalid")
)
