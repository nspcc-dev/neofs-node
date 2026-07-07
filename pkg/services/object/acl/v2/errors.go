package v2

import (
	"errors"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

const invalidRequestMessage = "malformed request"

var (
	// ErrNotMatched is returned from CheckEACL() when there were no rules
	// found that match request/object. Most of the time this means there
	// were no object headers to check rules against, but it can also
	// mean the default behavior after the full table scan.
	ErrNotMatched = errors.New("no matching rule")

	errInvalidVerb apistatus.ObjectAccessDenied
)

func init() {
	errInvalidVerb.WriteReason("session token verb is invalid")
}
