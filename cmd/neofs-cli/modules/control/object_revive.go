package control

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

var reviveObjectCmd = &cobra.Command{
	Use:   "revive",
	Short: "Forcefully revive object",
	Long:  "Purge removal marks from metabases",
	Args:  cobra.NoArgs,
	RunE:  reviveObject,
}

func initControlObjectReviveCmd() {
	initControlFlags(reviveObjectCmd)

	flags := reviveObjectCmd.Flags()
	flags.String(objectFlag, "", "Object address")
}

func reviveObject(cmd *cobra.Command, _ []string) error {
	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}
	addressRaw, err := cmd.Flags().GetString(objectFlag)
	if err != nil {
		return fmt.Errorf("reading %s flag: %w", objectFlag, err)
	}

	req := &control.ReviveObjectRequest{
		Body: &control.ReviveObjectRequest_Body{
			ObjectAddress: []byte(addressRaw),
		},
	}
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

	resp, err := cli.ReviveObject(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	shards := resp.GetBody().GetShards()
	if len(shards) == 0 {
		cmd.Println("<empty response>")
		return nil
	}

	for _, shard := range shards {
		cmd.Printf("Shard ID: %s\n", shard.ShardId)
		cmd.Printf("Revival status: %s\n", shard.Status)
	}

	return nil
}
