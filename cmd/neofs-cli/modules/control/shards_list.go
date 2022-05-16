package control

import (
	"bytes"
	"encoding/json"
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
	flags.Bool(commonflags.JSON, false, "Print shard info as a JSON array")
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

	isJSON, _ := cmd.Flags().GetBool(commonflags.JSON)
	if isJSON {
		prettyPrintShardsJSON(cmd, resp.GetBody().GetShards())
	} else {
		prettyPrintShards(cmd, resp.GetBody().GetShards())
	}
}

func prettyPrintShardsJSON(cmd *cobra.Command, ii []*control.ShardInfo) {
	out := make([]map[string]interface{}, 0, len(ii))
	for _, i := range ii {
		out = append(out, map[string]interface{}{
			"shard_id":    base58.Encode(i.Shard_ID),
			"mode":        shardModeToString(i.GetMode()),
			"metabase":    i.GetMetabasePath(),
			"blobstor":    i.GetBlobstorPath(),
			"writecache":  i.GetWritecachePath(),
			"error_count": i.GetErrorCount(),
		})
	}

	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")
	common.ExitOnErr(cmd, "cannot shard info to JSON: %w", enc.Encode(out))

	cmd.Print(buf.String()) // pretty printer emits newline, to no need for Println
}

func prettyPrintShards(cmd *cobra.Command, ii []*control.ShardInfo) {
	for _, i := range ii {
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
			pathPrinter("Pilorama", i.GetPiloramaPath())+
			fmt.Sprintf("Error count: %d\n", i.GetErrorCount()),
			base58.Encode(i.Shard_ID),
			shardModeToString(i.GetMode()),
		)
	}
}

func shardModeToString(m control.ShardMode) string {
	switch m {
	case control.ShardMode_READ_WRITE:
		return "read-write"
	case control.ShardMode_READ_ONLY:
		return "read-only"
	case control.ShardMode_DEGRADED:
		return "degraded"
	default:
		return "unknown"
	}
}
