package commonflags

import "github.com/spf13/cobra"

const SessionToken = "session"

// InitSession initializes session parameter for cmd.
func InitSession(cmd *cobra.Command) {
	cmd.Flags().String(
		SessionToken,
		"",
		"path to a JSON-encoded container session token",
	)
}
