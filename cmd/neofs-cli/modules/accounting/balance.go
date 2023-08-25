package accounting

import (
	"context"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
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
	Run: func(cmd *cobra.Command, _ []string) {
		ctx := context.Background()

		var idUser user.ID

		pk := key.GetOrGenerate(cmd)

		balanceOwner, _ := cmd.Flags().GetString(ownerFlag)
		if balanceOwner == "" {
			idUser = user.ResolveFromECDSAPublicKey(pk.PublicKey)
		} else {
			common.ExitOnErr(cmd, "can't decode owner ID wallet address: %w", idUser.DecodeString(balanceOwner))
		}

		cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

		var prm internalclient.BalanceOfPrm
		prm.SetClient(cli)
		prm.SetAccount(idUser)

		res, err := internalclient.BalanceOf(ctx, prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		// print to stdout
		prettyPrintDecimal(cmd, res.Balance())
	},
}

func initAccountingBalanceCmd() {
	ff := accountingBalanceCmd.Flags()

	ff.StringP(commonflags.WalletPath, commonflags.WalletPathShorthand, commonflags.WalletPathDefault, commonflags.WalletPathUsage)
	ff.StringP(commonflags.Account, commonflags.AccountShorthand, commonflags.AccountDefault, commonflags.AccountUsage)
	ff.StringP(commonflags.RPC, commonflags.RPCShorthand, commonflags.RPCDefault, commonflags.RPCUsage)
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
