package control

import (
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/spf13/cobra"
)

const (
	dumpFilepathFlag     = "path"
	dumpIgnoreErrorsFlag = "no-errors"
)

var dumpShardCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump objects from shard",
	Long:  "Dump objects from shard to a file",
	Args:  cobra.NoArgs,
	Run:   dumpShard,
}

func dumpShard(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)

	body := new(control.DumpShardRequest_Body)
	body.SetShardID(getShardID(cmd))

	p, _ := cmd.Flags().GetString(dumpFilepathFlag)
	body.SetFilepath(p)

	ignore, _ := cmd.Flags().GetBool(dumpIgnoreErrorsFlag)
	body.SetIgnoreErrors(ignore)

	req := new(control.DumpShardRequest)
	req.SetBody(body)

	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

	var resp *control.DumpShardResponse
	var err error
	err = cli.ExecRaw(func(client *client.Client) error {
		resp, err = control.DumpShard(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Shard has been dumped successfully.")
}

func initControlDumpShardCmd() {
	initControlFlags(dumpShardCmd)

	flags := dumpShardCmd.Flags()
	flags.String(shardIDFlag, "", "Shard ID in base58 encoding")
	flags.String(dumpFilepathFlag, "", "File to write objects to")
	flags.Bool(dumpIgnoreErrorsFlag, false, "Skip invalid/unreadable objects")

	_ = dumpShardCmd.MarkFlagRequired(shardIDFlag)
	_ = dumpShardCmd.MarkFlagRequired(dumpFilepathFlag)
	_ = dumpShardCmd.MarkFlagRequired(controlRPC)
}
