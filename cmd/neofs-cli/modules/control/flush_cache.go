package control

import (
	"fmt"

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
	RunE:  flushCache,
}

func flushCache(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	req := &control.FlushCacheRequest{Body: new(control.FlushCacheRequest_Body)}
	req.Body.Shard_ID, err = getShardIDList(cmd)
	if err != nil {
		return err
	}

	err = signRequest(pk, req)
	if err != nil {
		return err
	}

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	resp, err := cli.FlushCache(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	cmd.Println("Write-cache has been flushed.")
	return nil
}

func initControlFlushCacheCmd() {
	initControlFlags(flushCacheCmd)

	ff := flushCacheCmd.Flags()
	ff.StringSlice(shardIDFlag, nil, "List of shard IDs in base58 encoding")
	ff.Bool(shardAllFlag, false, "Process all shards")

	flushCacheCmd.MarkFlagsOneRequired(shardIDFlag, shardAllFlag)
}
