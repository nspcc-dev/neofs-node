package netmap

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "netmap",
	Short: "Operations with Network Map",
	Long:  `Operations with Network Map`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// bind exactly that cmd's flags to
		// the viper before execution
		commonflags.Bind(cmd)
		commonflags.BindAPI(cmd)
	},
}

func init() {
	Cmd.AddCommand(
		getEpochCmd,
		nodeInfoCmd,
		netInfoCmd,
		snapshotCmd,
	)

	initGetEpochCmd()
	initNetInfoCmd()
	initNodeInfoCmd()
	initSnapshotCmd()
}
