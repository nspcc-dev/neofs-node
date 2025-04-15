package control

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

var evacuateShardCmd = &cobra.Command{
	Use:   "evacuate",
	Short: "Evacuate objects from shard",
	Long:  "Evacuate objects from shard to other shards",
	Args:  cobra.NoArgs,
	RunE:  evacuateShard,
}

func evacuateShard(cmd *cobra.Command, _ []string) error {
	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	req := &control.EvacuateShardRequest{Body: new(control.EvacuateShardRequest_Body)}
	req.Body.Shard_ID, err = getShardIDList(cmd)
	if err != nil {
		return err
	}
	req.Body.IgnoreErrors, _ = cmd.Flags().GetBool(dumpIgnoreErrorsFlag)

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

	resp, err := cli.EvacuateShard(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	cmd.Printf("Objects moved: %d\n", resp.GetBody().GetCount())

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	cmd.Println("Shard has successfully been evacuated.")
	return nil
}

func initControlEvacuateShardCmd() {
	initControlFlags(evacuateShardCmd)

	flags := evacuateShardCmd.Flags()
	flags.StringSlice(shardIDFlag, nil, "List of shard IDs in base58 encoding")
	flags.Bool(shardAllFlag, false, "Process all shards")
	flags.Bool(dumpIgnoreErrorsFlag, false, "Skip invalid/unreadable objects")

	evacuateShardCmd.MarkFlagsOneRequired(shardIDFlag, shardAllFlag)
}
