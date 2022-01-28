package cmd

import (
	"fmt"
	"math"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-sdk-go/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	balanceOwner string
)

// accountingCmd represents the accounting command
var accountingCmd = &cobra.Command{
	Use:   "accounting",
	Short: "Operations with accounts and balances",
	Long:  `Operations with accounts and balances`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		flags := cmd.Flags()

		_ = viper.BindPFlag(binaryKey, flags.Lookup(binaryKey))
		_ = viper.BindPFlag(walletPath, flags.Lookup(walletPath))
		_ = viper.BindPFlag(wif, flags.Lookup(wif))
		_ = viper.BindPFlag(address, flags.Lookup(address))
		_ = viper.BindPFlag(rpc, flags.Lookup(rpc))
	},
}

var accountingBalanceCmd = &cobra.Command{
	Use:   "balance",
	Short: "Get internal balance of NeoFS account",
	Long:  `Get internal balance of NeoFS account`,
	Run: func(cmd *cobra.Command, args []string) {
		var oid *owner.ID

		key, err := getKey()
		exitOnErr(cmd, err)

		if balanceOwner == "" {
			wallet, err := owner.NEO3WalletFromPublicKey(&key.PublicKey)
			exitOnErr(cmd, err)

			oid = owner.NewIDFromNeo3Wallet(wallet)
		} else {
			oid, err = ownerFromString(balanceOwner)
			exitOnErr(cmd, err)
		}

		var prm internalclient.BalanceOfPrm

		prepareAPIClientWithKey(cmd, key, &prm)
		prm.SetOwner(oid)

		res, err := internalclient.BalanceOf(prm)
		exitOnErr(cmd, errf("rpc error: %w", err))

		// print to stdout
		prettyPrintDecimal(cmd, res.Balance())
	},
}

func initAccountingBalanceCmd() {
	ff := accountingBalanceCmd.Flags()

	ff.StringP(binaryKey, binaryKeyShorthand, binaryKeyDefault, binaryKeyUsage)
	ff.StringP(walletPath, walletPathShorthand, walletPathDefault, walletPathUsage)
	ff.StringP(wif, wifShorthand, wifDefault, wifUsage)
	ff.StringP(address, addressShorthand, addressDefault, addressUsage)
	ff.StringP(rpc, rpcShorthand, rpcDefault, rpcUsage)

	accountingBalanceCmd.Flags().StringVar(&balanceOwner, "owner", "", "owner of balance account (omit to use owner from private key)")
}

func init() {
	rootCmd.AddCommand(accountingCmd)
	accountingCmd.AddCommand(accountingBalanceCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// accountingCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// accountingCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	initAccountingBalanceCmd()
}

func prettyPrintDecimal(cmd *cobra.Command, decimal *accounting.Decimal) {
	if decimal == nil {
		return
	}

	if viper.GetBool(verbose) {
		cmd.Println("value:", decimal.Value())
		cmd.Println("precision:", decimal.Precision())
	} else {
		// divider = 10^{precision}; v:365, p:2 => 365 / 10^2 = 3.65
		divider := math.Pow(10, float64(decimal.Precision()))

		// %0.8f\n for precision 8
		format := fmt.Sprintf("%%0.%df\n", decimal.Precision())

		cmd.Printf(format, float64(decimal.Value())/divider)
	}
}
