package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/internal/cmdprinter"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/spf13/cobra"
)

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Request current local snapshot of the network map",
	Long:  `Request current local snapshot of the network map`,
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx, cancel := commonflags.GetCommandContext(cmd)
		defer cancel()

		cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
		if err != nil {
			return err
		}
		defer cli.Close()

		res, err := cli.NetMapSnapshot(ctx, client.PrmNetMapSnapshot{})
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}

		cmdprinter.PrettyPrintNetMap(cmd, res)
		return nil
	},
}

func initSnapshotCmd() {
	commonflags.Init(snapshotCmd)
	commonflags.InitAPI(snapshotCmd)
}
