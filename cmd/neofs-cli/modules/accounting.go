package cmd

import (
	"context"
	"fmt"
	"math"

	"github.com/nspcc-dev/neofs-api-go/pkg/accounting"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/spf13/cobra"
)

var (
	balanceOwner string
)

// accountingCmd represents the accounting command
var accountingCmd = &cobra.Command{
	Use:   "accounting",
	Short: "Operations with accounts and balances",
	Long:  `Operations with accounts and balances`,
}

var accountingBalanceCmd = &cobra.Command{
	Use:   "balance",
	Short: "Get internal balance of NeoFS account",
	Long:  `Get internal balance of NeoFS account`,
	Run: func(cmd *cobra.Command, args []string) {
		var (
			response *accounting.Decimal
			oid      *owner.ID

			ctx = context.Background()
		)

		key, err := getKey()
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		cli, err := getSDKClient(key)
		if err != nil {
			cmd.PrintErrln(err)
			return
		}

		if balanceOwner == "" {
			wallet, err := owner.NEO3WalletFromPublicKey(&key.PublicKey)
			if err != nil {
				cmd.PrintErrln(err)
				return
			}

			oid = owner.NewIDFromNeo3Wallet(wallet)
		} else {
			oid, err = ownerFromString(balanceOwner)
			if err != nil {
				cmd.PrintErrln(err)
				return
			}
		}

		response, err = cli.GetBalance(ctx, oid, globalCallOptions()...)
		if err != nil {
			cmd.PrintErrln(fmt.Errorf("rpc error: %w", err))
			return
		}

		// print to stdout
		prettyPrintDecimal(cmd, response)
	},
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

	accountingBalanceCmd.Flags().StringVar(&balanceOwner, "owner", "", "owner of balance account (omit to use owner from private key)")
}

func prettyPrintDecimal(cmd *cobra.Command, decimal *accounting.Decimal) {
	if decimal == nil {
		return
	}

	if verbose {
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
