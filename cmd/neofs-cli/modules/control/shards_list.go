package control

import (
	"fmt"

	"github.com/mr-tron/base58"
	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

var listShardsCmd = &cobra.Command{
	Use:   "list",
	Short: "List shards of the storage node",
	Long:  "List shards of the storage node",
	Run:   listShards,
}

func initControlShardsListCmd() {
	commonflags.InitWithoutRPC(listShardsCmd)

	flags := listShardsCmd.Flags()

	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
}

func listShards(cmd *cobra.Command, _ []string) {
	pk := key.Get(cmd)

	req := new(control.ListShardsRequest)
	req.SetBody(new(control.ListShardsRequest_Body))

	signRequest(cmd, pk, req)

	cli := getClient(cmd, pk)

	var resp *control.ListShardsResponse
	var err error
	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.ListShards(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	prettyPrintShards(cmd, resp.GetBody().GetShards())
}

func prettyPrintShards(cmd *cobra.Command, ii []*control.ShardInfo) {
	for _, i := range ii {
		var mode string

		switch i.GetMode() {
		case control.ShardMode_READ_WRITE:
			mode = "read-write"
		case control.ShardMode_READ_ONLY:
			mode = "read-only"
		case control.ShardMode_DEGRADED:
			mode = "degraded"
		default:
			mode = "unknown"
		}

		pathPrinter := func(name, path string) string {
			if path == "" {
				return ""
			}

			return fmt.Sprintf("%s: %s\n", name, path)
		}

		cmd.Printf("Shard %s:\nMode: %s\n"+
			pathPrinter("Metabase", i.GetMetabasePath())+
			pathPrinter("Blobstor", i.GetBlobstorPath())+
			pathPrinter("Write-cache", i.GetWritecachePath())+
			fmt.Sprintf("Error count: %d\n", i.GetErrorCount()),
			base58.Encode(i.Shard_ID),
			mode,
		)
	}
}
