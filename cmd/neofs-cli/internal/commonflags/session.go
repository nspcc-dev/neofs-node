package commonflags

import (
	"fmt"

	"github.com/spf13/cobra"
)

const SessionToken = "session"

// InitSession registers SessionToken flag representing filepath to the token
// of the session with the given name. Supports NeoFS-binary and JSON files.
func InitSession(cmd *cobra.Command, name string) {
	cmd.Flags().String(
		SessionToken,
		"",
		fmt.Sprintf("Filepath to a JSON- or binary-encoded token of the %s session", name),
	)
}
