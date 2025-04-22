package request

import (
	"time"

	"github.com/spf13/cobra"
)

// FIXME: do not inherit global flags
// neofs-cli request -h
// Global Flags:
//  -c, --config string   Config file (default is $HOME/.config/neofs-cli/config.yaml)
//  -v, --verbose         Verbose output

const (
	defaultDialTimeout    = 5 * time.Second
	defaultRequestTimeout = 5 * time.Second
)

// Cmd is a command to send request to NeoFS.
var Cmd = &cobra.Command{
	Use:   "request",
	Short: "Send request to NeoFS",
	Long:  "Send request to NeoFS",
	Args:  cobra.NoArgs,
}

func init() {
	Cmd.AddCommand(
		createContainerCmd,
	)
}
