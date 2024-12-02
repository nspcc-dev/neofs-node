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

var notarySignHashFlag string

var notarySignCmd = &cobra.Command{
	Use:   "sign",
	Short: "Sign notary request by its hash",
	Long:  "Sign notary request by its hash",
	Args:  cobra.NoArgs,
	RunE:  notarySign,
}

func initControlNotarySignCmd() {
	initControlFlags(notarySignCmd)

	flags := notarySignCmd.Flags()
	flags.StringVar(&notarySignHashFlag, "hash", "", "hash of the notary request")
}

func notarySign(cmd *cobra.Command, _ []string) error {
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

	req := new(ircontrol.NotarySignRequest)
	body := new(ircontrol.NotarySignRequest_Body)
	req.SetBody(body)
	hash, err := util.Uint256DecodeStringBE(notarySignHashFlag)
	if err != nil {
		return fmt.Errorf("failed to decode hash: %w", err)
	}
	body.SetHash(hash.BytesBE())

	err = ircontrolsrv.SignMessage(pk, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	var resp *ircontrol.NotarySignResponse
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = ircontrol.NotarySign(client, req)
		return err
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	return nil
}
