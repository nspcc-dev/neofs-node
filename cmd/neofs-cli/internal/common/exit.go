package common

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	sdkstatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// ErrAwaitTimeout represents the expiration of a polling interval
// while awaiting a certain condition.
var ErrAwaitTimeout = errors.New("await timeout expired")

// WrapError wrap error to cmderr.ExitErr, if it not nil, and add a code depending on the error type
// Codes:
//
//	1 if [sdkstatus.ErrServerInternal] or untyped
//	2 if [sdkstatus.ErrObjectAccessDenied]
//	3 if [ErrAwaitTimeout]
func WrapError(err error) error {
	if err == nil {
		return nil
	}

	const (
		_ = iota
		internal
		aclDenied
		awaitTimeout
		alreadyRemoved
	)

	var code int
	var accessErr = new(sdkstatus.ObjectAccessDenied)

	switch {
	case errors.Is(err, sdkstatus.ErrServerInternal):
		code = internal
	case errors.As(err, &accessErr):
		code = aclDenied
		err = fmt.Errorf("%w: %s", err, accessErr.Reason())
	case errors.Is(err, ErrAwaitTimeout):
		code = awaitTimeout
	case errors.Is(err, sdkstatus.ErrObjectAlreadyRemoved):
		code = alreadyRemoved
	default:
		code = internal
	}

	return cmderr.ExitErr{Code: code, Cause: err}
}
