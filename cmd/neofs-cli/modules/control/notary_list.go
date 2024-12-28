package control

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	ircontrol "github.com/nspcc-dev/neofs-node/pkg/services/control/ir"
	ircontrolsrv "github.com/nspcc-dev/neofs-node/pkg/services/control/ir/server"
	"github.com/spf13/cobra"
)

var listNotaryCmd = &cobra.Command{
	Use:   "list",
	Short: "Get list of all notary requests in network",
	Long:  "Get list of all notary requests in network",
	Args:  cobra.NoArgs,
	RunE:  listNotary,
}

func initControlNotaryListCmd() {
	initControlFlags(listNotaryCmd)
}

func listNotary(cmd *cobra.Command, _ []string) error {
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

	req := new(ircontrol.NotaryListRequest)

	req.SetBody(new(ircontrol.NotaryListRequest_Body))

	err = ircontrolsrv.SignMessage(pk, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	var resp *ircontrol.NotaryListResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = ircontrol.NotaryList(client, req)
		return err
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	txs := resp.GetBody().GetTransactions()

	for _, tx := range txs {
		hash, err := util.Uint256DecodeBytesBE(tx.GetHash())
		if err != nil {
			return fmt.Errorf("failed to decode hash: %w", err)
		}
		cmd.Println(hash.String())
	}
	return nil
}