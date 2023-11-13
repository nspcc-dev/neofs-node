package util

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Cmd = &cobra.Command{
	Use:   "util",
	Short: "Utility operations",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()

		_ = viper.BindPFlag(commonflags.GenerateKey, flags.Lookup(commonflags.GenerateKey))
		_ = viper.BindPFlag(commonflags.WalletPath, flags.Lookup(commonflags.WalletPath))
		_ = viper.BindPFlag(commonflags.Account, flags.Lookup(commonflags.Account))
	},
}

func init() {
	Cmd.AddCommand(
		signCmd,
		convertCmd,
		keyerCmd,
	)

	initSignCmd()
	initConvertCmd()
	initKeyerCmd()
}
