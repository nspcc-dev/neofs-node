package policy

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
)

// Cmd represents the policy command.
var Cmd = &cobra.Command{
	Use:   "policy",
	Short: "Operations with container placement policy",
	Long:  "Operations with container placement policy",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		commonflags.Bind(cmd)
		commonflags.BindAPI(cmd)
	},
}

func init() {
	Cmd.AddCommand(checkCmd)
	initCheckCmd()
}
