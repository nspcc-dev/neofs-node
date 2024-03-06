package control

import (
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
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
	Run:   evacuateShard,
}

func evacuateShard(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)

	req := &control.EvacuateShardRequest{Body: new(control.EvacuateShardRequest_Body)}
	req.Body.Shard_ID = getShardIDList(cmd)
	req.Body.IgnoreErrors, _ = cmd.Flags().GetBool(dumpIgnoreErrorsFlag)

	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

	var resp *control.EvacuateShardResponse
	var err error
	err = cli.ExecRaw(func(client *client.Client) error {
		resp, err = control.EvacuateShard(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	cmd.Printf("Objects moved: %d\n", resp.GetBody().GetCount())

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Shard has successfully been evacuated.")
}

func initControlEvacuateShardCmd() {
	initControlFlags(evacuateShardCmd)

	flags := evacuateShardCmd.Flags()
	flags.StringSlice(shardIDFlag, nil, "List of shard IDs in base58 encoding")
	flags.Bool(shardAllFlag, false, "Process all shards")
	flags.Bool(dumpIgnoreErrorsFlag, false, "Skip invalid/unreadable objects")

	evacuateShardCmd.MarkFlagsOneRequired(shardIDFlag, shardAllFlag)
}
