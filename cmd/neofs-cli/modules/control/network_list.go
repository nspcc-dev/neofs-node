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

var listNetworkCmd = &cobra.Command{
	Use:   "list",
	Short: "Get list of all requested changes in network",
	Long:  "Get list of all requested changes in network",
	Args:  cobra.NoArgs,
	RunE:  listNetwork,
}

func initControlNetworkListCmd() {
	initControlFlags(listNetworkCmd)
}

func listNetwork(cmd *cobra.Command, _ []string) error {
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

	req := new(ircontrol.NetworkListRequest)

	req.SetBody(new(ircontrol.NetworkListRequest_Body))

	err = ircontrolsrv.SignMessage(pk, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	var resp *ircontrol.NetworkListResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = ircontrol.NetworkList(client, req)
		return err
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	hashes := resp.GetBody().GetHashes()

	cmd.Printf("Hashes: %s\n", hashes)
	return nil
}
