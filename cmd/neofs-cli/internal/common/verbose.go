package common

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/viper"
)

// PrintVerbose prints to the stdout if the commonflags.Verbose flag is on.
func PrintVerbose(format string, a ...interface{}) {
	if viper.GetBool(commonflags.Verbose) {
		fmt.Printf(format+"\n", a...)
	}
}
