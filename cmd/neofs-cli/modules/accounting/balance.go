package accounting

import (
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
	Run: func(cmd *cobra.Command, args []string) {
		var oid user.ID

		pk := key.GetOrGenerate(cmd)

		balanceOwner, _ := cmd.Flags().GetString(ownerFlag)
		if balanceOwner == "" {
			user.IDFromKey(&oid, pk.PublicKey)
		} else {
			common.ExitOnErr(cmd, "can't decode owner ID wallet address: %w", oid.DecodeString(balanceOwner))
		}

		cli, err := internalclient.GetSDKClientByFlag(pk, commonflags.RPC)
		common.ExitOnErr(cmd, "create API client: %w", err)

		var prm internalclient.BalanceOfPrm
		prm.SetClient(cli)
		prm.SetAccount(oid)

		res, err := internalclient.BalanceOf(prm)
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

func prettyPrintDecimal(cmd *cobra.Command, decimal *accounting.Decimal) {
	if decimal == nil {
		return
	}

	if viper.GetBool(commonflags.Verbose) {
		cmd.Println("value:", decimal.Value())
		cmd.Println("precision:", decimal.Precision())
	} else {
		amountF8 := precision.Convert(decimal.Precision(), 8, big.NewInt(decimal.Value()))

		cmd.Println(fixedn.ToString(amountF8, 8))
	}
}
