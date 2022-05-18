package common

import (
	"errors"
	"fmt"
	"os"

	sdkstatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/spf13/cobra"
)

// ExitOnErr prints error and exits with a code that matches
// one of the common errors from sdk library. If no errors
// found, exits with 1 code.
// Does nothing if passed error in nil.
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
	)

	var (
		code int

		internalErr = new(sdkstatus.ServerInternal)
		accessErr   = new(sdkstatus.ObjectAccessDenied)
	)

	switch {
	case errors.As(err, &internalErr):
		code = internal
	case errors.As(err, &accessErr):
		code = aclDenied
		err = fmt.Errorf("%w: %s", err, accessErr.Reason())
	default:
		code = internal
	}

	cmd.PrintErrln(err)
	os.Exit(code)
}
