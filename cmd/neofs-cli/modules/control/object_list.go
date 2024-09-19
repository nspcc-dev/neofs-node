package control

import (
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
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
	Run:   listObjects,
}

func initControlObjectsListCmd() {
	initControlFlags(listObjectsCmd)
}

func listObjects(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)

	req := &control.ListObjectsRequest{
		Body: &control.ListObjectsRequest_Body{},
	}
	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

	err := cli.ExecRaw(func(client *rawclient.Client) error {
		return control.ListObjects(client, req, func(r *control.ListObjectsResponse) {
			verifyResponse(cmd, r.GetSignature(), r.GetBody())

			for _, address := range r.GetBody().GetObjectAddress() {
				cmd.Println(string(address))
			}
		})
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)
}
