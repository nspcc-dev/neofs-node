package common

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Errf returns formatted error in errFmt format if err is not nil.
func Errf(errFmt string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf(errFmt, err)
}

// ExitOnErr calls exitOnErrCode with code 1.
func ExitOnErr(cmd *cobra.Command, err error) {
	exitOnErrCode(cmd, err, 1)
}

// exitOnErrCode prints error via cmd and calls os.Exit with passed exit code.
// Does nothing if err is nil.
func exitOnErrCode(cmd *cobra.Command, err error, code int) {
	if err != nil {
		cmd.PrintErrln(err)
		os.Exit(code)
	}
}
