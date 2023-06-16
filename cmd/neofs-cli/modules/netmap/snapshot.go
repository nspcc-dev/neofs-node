package netmap

import (
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Request current local snapshot of the network map",
	Long:  `Request current local snapshot of the network map`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		p := key.GetOrGenerate(cmd)
		cli := internalclient.GetSDKClientByFlag(ctx, cmd, p, commonflags.RPC)

		var prm internalclient.NetMapSnapshotPrm
		prm.SetClient(cli)

		res, err := internalclient.NetMapSnapshot(ctx, prm)
		common.ExitOnErr(cmd, "rpc error: %w", err)

		common.PrettyPrintNetMap(cmd, res.NetMap())
	},
}

func initSnapshotCmd() {
	commonflags.Init(snapshotCmd)
	commonflags.InitAPI(snapshotCmd)
}
