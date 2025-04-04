package control

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Cmd = &cobra.Command{
	Use:   "control",
	Short: "Operations with storage node",
	Long:  `Operations with storage node`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		ff := cmd.Flags()

		_ = viper.BindPFlag(commonflags.WalletPath, ff.Lookup(commonflags.WalletPath))
		_ = viper.BindPFlag(commonflags.Account, ff.Lookup(commonflags.Account))
		_ = viper.BindPFlag(controlRPC, ff.Lookup(controlRPC))
		_ = viper.BindPFlag(commonflags.Timeout, ff.Lookup(commonflags.Timeout))
	},
}

const (
	controlRPC        = "endpoint"
	controlRPCDefault = ""
	controlRPCUsage   = "Remote node control address (as 'multiaddr' or '<host>:<port>')"
)

func init() {
	Cmd.AddCommand(
		healthCheckCmd,
		setNetmapStatusCmd,
		dropObjectsCmd,
		shardsCmd,
		objectCmd,
		notaryCmd,
	)

	initControlHealthCheckCmd()
	initControlSetNetmapStatusCmd()
	initControlDropObjectsCmd()
	initControlShardsCmd()
	initControlObjectsCmd()
	initControlNotaryCmd()
}
