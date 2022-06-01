package common

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/spf13/cobra"
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

// PrintChecksum prints checksum.
func PrintChecksum(cmd *cobra.Command, name string, recv func() (checksum.Checksum, bool)) {
	var strVal string

	cs, csSet := recv()
	if csSet {
		strVal = hex.EncodeToString(cs.Value())
	} else {
		strVal = "<empty>"
	}

	cmd.Printf("%s: %s\n", name, strVal)
}
