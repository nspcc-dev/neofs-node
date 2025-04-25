package accounting

import (
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/util/precision"
	"github.com/nspcc-dev/neofs-sdk-go/accounting"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	ownerFlag = "owner"
)

var accountingBalanceCmd = &cobra.Command{
	Use:   "balance",
	Short: "Get internal balance of NeoFS account",
	Long:  `Get internal balance of NeoFS account`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		var idUser user.ID

		pk, err := key.GetOrGenerate(cmd)
		if err != nil {
			return err
		}

		balanceOwner, _ := cmd.Flags().GetString(ownerFlag)
		if balanceOwner == "" {
			idUser = user.NewFromECDSAPublicKey(pk.PublicKey)
		} else {
			err = idUser.DecodeString(balanceOwner)
			if err != nil {
				return fmt.Errorf("can't decode owner ID wallet address: %w", err)
			}
		}

		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		var prm internalclient.BalanceOfPrm
		prm.SetClient(cli)
		prm.SetAccount(idUser)

		res, err := internalclient.BalanceOf(ctx, prm)
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		// print to stdout
		prettyPrintDecimal(cmd, res.Balance())
		return nil
	},
}

func initAccountingBalanceCmd() {
	commonflags.Init(accountingBalanceCmd)

	ff := accountingBalanceCmd.Flags()

	ff.String(ownerFlag, "", "owner of balance account (omit to use owner from private key)")
}

func prettyPrintDecimal(cmd *cobra.Command, decimal accounting.Decimal) {
	if viper.GetBool(commonflags.Verbose) {
		cmd.Println("value:", decimal.Value())
		cmd.Println("precision:", decimal.Precision())
	} else {
		amountF8 := precision.Convert(decimal.Precision(), 8, big.NewInt(decimal.Value()))

		cmd.Println(fixedn.ToString(amountF8, 8))
	}
}
