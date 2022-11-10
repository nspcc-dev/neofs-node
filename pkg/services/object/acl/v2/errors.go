package v2

import (
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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
	errInvalidSessionOwner     = malformedRequestError("invalid session token owner")
	errInvalidVerb             = malformedRequestError("session token verb is invalid")
)

const accessDeniedACLReasonFmt = "access to operation %s is denied by basic ACL check"
const accessDeniedEACLReasonFmt = "access to operation %s is denied by extended ACL check: %v"

func basicACLErr(info RequestInfo) error {
	var errAccessDenied apistatus.ObjectAccessDenied
	errAccessDenied.WriteReason(fmt.Sprintf(accessDeniedACLReasonFmt, info.operation))

	return errAccessDenied
}

func eACLErr(info RequestInfo, err error) error {
	var errAccessDenied apistatus.ObjectAccessDenied
	errAccessDenied.WriteReason(fmt.Sprintf(accessDeniedEACLReasonFmt, info.operation, err))

	return errAccessDenied
}
