package netmap

import (
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/spf13/cobra"
)

var getEpochCmd = &cobra.Command{
	Use:   "epoch",
	Short: "Get current epoch number",
	Long:  "Get current epoch number",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		p := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(ctx, cmd, p, commonflags.RPC)

		var prm internalclient.NetworkInfoPrm
		prm.SetClient(cli)

		res, err := internalclient.NetworkInfo(ctx, prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		netInfo := res.NetworkInfo()

		cmd.Println(netInfo.CurrentEpoch())
	},
}

func initGetEpochCmd() {
	commonflags.Init(getEpochCmd)
	commonflags.InitAPI(getEpochCmd)
}
