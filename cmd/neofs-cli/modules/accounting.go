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
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			response *accounting.Decimal
			oid      *owner.ID
			err      error

			ctx = context.Background()
		)

		cli, err := getSDKClient()
		if err != nil {
			return err
		}

		switch balanceOwner {
		case "":
			response, err = cli.GetSelfBalance(ctx)
		default:
			oid, err = ownerFromString(balanceOwner)
			if err != nil {
				return err
			}

			response, err = cli.GetBalance(ctx, oid)
		}

		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		// print to stdout
		prettyPrintDecimal(response)

		return nil
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

func prettyPrintDecimal(decimal *accounting.Decimal) {
	if decimal == nil {
		return
	}

	if verbose {
		fmt.Println("value:", decimal.GetValue())
		fmt.Println("precision:", decimal.GetPrecision())
	} else {
		// divider = 10^{precision}; v:365, p:2 => 365 / 10^2 = 3.65
		divider := math.Pow(10, float64(decimal.GetPrecision()))

		// %0.8f\n for precision 8
		format := fmt.Sprintf("%%0.%df\n", decimal.GetPrecision())

		fmt.Printf(format, float64(decimal.GetValue())/divider)
	}
}
