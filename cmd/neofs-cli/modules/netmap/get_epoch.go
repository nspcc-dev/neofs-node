package netmap

import (
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/spf13/cobra"
)

var getEpochCmd = &cobra.Command{
	Use:   "epoch",
	Short: "Get current epoch number",
	Long:  "Get current epoch number",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		netInfo, err := cli.NetworkInfo(ctx, client.PrmNetworkInfo{})
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		cmd.Println(netInfo.CurrentEpoch())
		return nil
	},
}

func initGetEpochCmd() {
	commonflags.Init(getEpochCmd)
	commonflags.InitAPI(getEpochCmd)
}
