package control

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mr-tron/base58"
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
	RunE:  listShards,
}

func initControlShardsListCmd() {
	initControlFlags(listShardsCmd)

	flags := listShardsCmd.Flags()
	flags.Bool(commonflags.JSON, false, "Print shard info as a JSON array")
}

func listShards(cmd *cobra.Command, _ []string) error {
	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	var req = &control.ListShardsRequest{Body: new(control.ListShardsRequest_Body)}

	err = signRequest(pk, req)
	if err != nil {
		return err
	}

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	resp, err := cli.ListShards(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	isJSON, _ := cmd.Flags().GetBool(commonflags.JSON)
	if isJSON {
		if err := prettyPrintShardsJSON(cmd, resp.GetBody().GetShards()); err != nil {
			return err
		}
	} else {
		prettyPrintShards(cmd, resp.GetBody().GetShards())
	}
	return nil
}

func prettyPrintShardsJSON(cmd *cobra.Command, ii []*control.ShardInfo) error {
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
	if err := enc.Encode(out); err != nil {
		return fmt.Errorf("cannot shard info to JSON: %w", err)
	}

	cmd.Print(buf.String()) // pretty printer emits newline, to no need for Println
	return nil
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
		sb.WriteString(fmt.Sprintf("\tPath: %s\n\tType: %s\n",
			i.GetBlobstor().GetPath(), i.GetBlobstor().GetType()))

		cmd.Printf("Shard %s:\nMode: %s\n"+
			pathPrinter("Metabase", i.GetMetabasePath())+
			sb.String()+
			pathPrinter("Write-cache", i.GetWritecachePath())+
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
