package common

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/viper"
)

// PrintVerbose prints to the stdout if the commonflags.Verbose flag is on.
func PrintVerbose(format string, a ...interface{}) {
	if viper.GetBool(commonflags.Verbose) {
		fmt.Printf(format+"\n", a...)
	}
}

// PrettyPrintUnixTime interprets s as unix timestamp and prints it as
// a date. Is s is invalid, "malformed" is returned.
func PrettyPrintUnixTime(s string) string {
	unixTime, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return "malformed"
	}

	timestamp := time.Unix(unixTime, 0)

	return timestamp.String()
}
