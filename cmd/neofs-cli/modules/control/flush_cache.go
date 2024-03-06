package control

import (
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

var flushCacheCmd = &cobra.Command{
	Use:   "flush-cache",
	Short: "Flush objects from the write-cache to the main storage",
	Long:  "Flush objects from the write-cache to the main storage",
	Args:  cobra.NoArgs,
	Run:   flushCache,
}

func flushCache(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)

	req := &control.FlushCacheRequest{Body: new(control.FlushCacheRequest_Body)}
	req.Body.Shard_ID = getShardIDList(cmd)

	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

	var resp *control.FlushCacheResponse
	var err error
	err = cli.ExecRaw(func(client *client.Client) error {
		resp, err = control.FlushCache(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Write-cache has been flushed.")
}

func initControlFlushCacheCmd() {
	initControlFlags(flushCacheCmd)

	ff := flushCacheCmd.Flags()
	ff.StringSlice(shardIDFlag, nil, "List of shard IDs in base58 encoding")
	ff.Bool(shardAllFlag, false, "Process all shards")

	flushCacheCmd.MarkFlagsOneRequired(shardIDFlag, shardAllFlag)
}
