package cmderr

import (
	"errors"
	"fmt"
	"os"
)

// ExitErr specific error for ExitOnErr function that passes the exit code and error caused.
type ExitErr struct {
	Code  int
	Cause error
}

func (x ExitErr) Error() string { return x.Cause.Error() }

// ExitOnErr writes error to os.Stderr and calls os.Exit with passed exit code or by default 1.
// Does nothing if err is nil.
func ExitOnErr(err error) {
	if err != nil {
		var e ExitErr
		if !errors.As(err, &e) {
			e.Code = 1
		}
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(e.Code)
	}
}
