package control

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

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
	Args:  cobra.NoArgs,
	Run:   listShards,
}

func initControlShardsListCmd() {
	initControlFlags(listShardsCmd)

	flags := listShardsCmd.Flags()
	flags.Bool(commonflags.JSON, false, "Print shard info as a JSON array")
}

func listShards(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)

	req := new(control.ListShardsRequest)
	req.SetBody(new(control.ListShardsRequest_Body))

	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

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
	out := make([]map[string]any, 0, len(ii))
	for _, i := range ii {
		out = append(out, map[string]any{
			"shard_id":    base58.Encode(i.Shard_ID),
			"mode":        shardModeToString(i.GetMode()),
			"metabase":    i.GetMetabasePath(),
			"blobstor":    i.GetBlobstor(),
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

		var sb strings.Builder
		sb.WriteString("Blobstor:\n")
		for j, info := range i.GetBlobstor() {
			sb.WriteString(fmt.Sprintf("\tPath %d: %s\n\tType %d: %s\n",
				j, info.GetPath(), j, info.GetType()))
		}

		cmd.Printf("Shard %s:\nMode: %s\n"+
			pathPrinter("Metabase", i.GetMetabasePath())+
			sb.String()+
			pathPrinter("Write-cache", i.GetWritecachePath())+
			pathPrinter("Pilorama", i.GetPiloramaPath())+
			fmt.Sprintf("Error count: %d\n", i.GetErrorCount()),
			base58.Encode(i.Shard_ID),
			shardModeToString(i.GetMode()),
		)
	}
}

func shardModeToString(m control.ShardMode) string {
	strMode, ok := lookUpShardModeString(m)
	if ok {
		return strMode
	}

	return "unknown"
}
