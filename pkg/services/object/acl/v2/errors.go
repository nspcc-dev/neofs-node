package v2

import (
	"errors"
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

var (
	// ErrMalformedRequest is returned when request contains
	// invalid data.
	ErrMalformedRequest = errors.New("malformed request")
	// ErrUnknownRole is returned when role of the sender is unknown.
	ErrUnknownRole = errors.New("can't classify request sender")
	// ErrInvalidVerb is returned when session token verb doesn't include necessary operation.
	ErrInvalidVerb = errors.New("session token verb is invalid")
)

const accessDeniedReasonFmt = "access to operation %v is denied by %s check"

func basicACLErr(info RequestInfo) error {
	var errAccessDenied apistatus.ObjectAccessDenied
	errAccessDenied.WriteReason(fmt.Sprintf(accessDeniedReasonFmt, info.operation, "basic ACL"))

	return errAccessDenied
}

func eACLErr(info RequestInfo) error {
	var errAccessDenied apistatus.ObjectAccessDenied
	errAccessDenied.WriteReason(fmt.Sprintf(accessDeniedReasonFmt, info.operation, "extended ACL"))

	return errAccessDenied
}
