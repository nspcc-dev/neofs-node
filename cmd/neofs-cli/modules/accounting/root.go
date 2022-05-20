package accounting

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Cmd represents the accounting command
var Cmd = &cobra.Command{
	Use:   "accounting",
	Short: "Operations with accounts and balances",
	Long:  `Operations with accounts and balances`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()

		_ = viper.BindPFlag(commonflags.WalletPath, flags.Lookup(commonflags.WalletPath))
		_ = viper.BindPFlag(commonflags.Account, flags.Lookup(commonflags.Account))
		_ = viper.BindPFlag(commonflags.RPC, flags.Lookup(commonflags.RPC))
	},
}

func init() {
	Cmd.AddCommand(accountingBalanceCmd)

	initAccountingBalanceCmd()
}
