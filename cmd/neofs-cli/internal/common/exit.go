package common

import (
	"errors"
	"fmt"
	"os"

	sdkstatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/spf13/cobra"
)

// ErrAwaitTimeout represents the expiration of a polling interval
// while awaiting a certain condition.
var ErrAwaitTimeout = errors.New("await timeout expired")

// ExitOnErr prints error and exits with a code depending on the error type
//
//	0 if nil
//	1 if [sdkstatus.ErrServerInternal] or untyped
//	2 if [sdkstatus.ErrObjectAccessDenied]
//	3 if [ErrAwaitTimeout]
func ExitOnErr(cmd *cobra.Command, errFmt string, err error) {
	if err == nil {
		return
	}

	if errFmt != "" {
		err = fmt.Errorf(errFmt, err)
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
	var alreadyRemovedErr = new(sdkstatus.ObjectAlreadyRemoved)

	switch {
	case errors.Is(err, sdkstatus.ErrServerInternal):
		code = internal
	case errors.As(err, &accessErr):
		code = aclDenied
		err = fmt.Errorf("%w: %s", err, accessErr.Reason())
	case errors.Is(err, ErrAwaitTimeout):
		code = awaitTimeout
	case errors.As(err, alreadyRemovedErr):
		code = alreadyRemoved
	default:
		code = internal
	}

	cmd.PrintErrln(err)
	os.Exit(code)
}
