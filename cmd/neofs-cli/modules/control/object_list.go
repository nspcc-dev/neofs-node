package control

import (
	"fmt"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

var listObjectsCmd = &cobra.Command{
	Use:   "list",
	Short: "Get list of all objects in the storage node",
	Long:  "Get list of all objects in the storage node",
	Args:  cobra.NoArgs,
	RunE:  listObjects,
}

func initControlObjectsListCmd() {
	initControlFlags(listObjectsCmd)
}

func listObjects(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	req := &control.ListObjectsRequest{
		Body: &control.ListObjectsRequest_Body{},
	}
	err = signRequest(pk, req)
	if err != nil {
		return err
	}

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	err = cli.ExecRaw(func(client *rawclient.Client) error {
		return control.ListObjects(client, req, func(r *control.ListObjectsResponse) error {
			err := verifyResponse(r.GetSignature(), r.GetBody())
			if err != nil {
				return err
			}

			for _, address := range r.GetBody().GetObjectAddress() {
				cmd.Println(string(address))
			}
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	return nil
}
