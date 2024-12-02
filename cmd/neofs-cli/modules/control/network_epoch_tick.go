package control

import (
	"fmt"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	ircontrol "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	ircontrolsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	"github.com/spf13/cobra"
)

var networkEpochTickCmd = &cobra.Command{
	Use:   "epoch-tick",
	Short: "Request to tick epoch",
	Long:  "Request to tick epoch",
	Args:  cobra.NoArgs,
	RunE:  networkEpockTick,
}

func initNetworkEpochTickCmd() {
	initControlFlags(networkEpochTickCmd)
}

func networkEpockTick(cmd *cobra.Command, args []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	req := new(ircontrol.NetworkEpochTickRequest)

	req.SetBody(new(ircontrol.NetworkEpochTickRequest_Body))

	err = ircontrolsrv.SignMessage(pk, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	var resp *ircontrol.NetworkEpochTickResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = ircontrol.NetworkEpochTick(client, req)
		return err
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	hashes := resp.GetBody().GetHash()

	cmd.Printf("Epoch Tick Hash: %s\n", hashes)
	return nil
}
