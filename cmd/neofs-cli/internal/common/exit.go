package common

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmderr"
	"github.com/nspcc-dev/neofs-sdk-go/client/status"
)

// ErrAwaitTimeout represents the expiration of a polling interval
// while awaiting a certain condition.
var ErrAwaitTimeout = errors.New("await timeout expired")

// WrapError wrap error to cmderr.ExitErr, if it not nil, and add a code depending on the error type
// Codes:
//
//	1 if [apistatus.ErrServerInternal] or untyped
//	2 if [apistatus.ErrObjectAccessDenied]
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
		incomplete
		busy
	)

	var (
		accessErr = new(apistatus.ObjectAccessDenied)
		code      int
		hide      bool
	)

	switch {
	case errors.Is(err, apistatus.ErrServerInternal):
		code = internal
	case errors.As(err, &accessErr):
		code = aclDenied
		err = fmt.Errorf("%w: %s", err, accessErr.Reason())
	case errors.Is(err, ErrAwaitTimeout):
		code = awaitTimeout
	case errors.Is(err, apistatus.ErrObjectAlreadyRemoved):
		code = alreadyRemoved
	case errors.Is(err, apistatus.ErrIncomplete):
		code = incomplete
		hide = true // Everything relevant is printed by commands already.
	case errors.Is(err, apistatus.ErrBusy):
		code = busy
	default:
		code = internal
	}

	return cmderr.ExitErr{Code: code, Cause: err, Hide: hide}
}
