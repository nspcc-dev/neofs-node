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
	restoreFilepathFlag     = "path"
	restoreIgnoreErrorsFlag = "no-errors"
)

var restoreShardCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore objects from shard",
	Long:  "Restore objects from shard to a file",
	Args:  cobra.NoArgs,
	Run:   restoreShard,
}

func restoreShard(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)

	body := new(control.RestoreShardRequest_Body)
	body.SetShardID(getShardID(cmd))

	p, _ := cmd.Flags().GetString(restoreFilepathFlag)
	body.SetFilepath(p)

	ignore, _ := cmd.Flags().GetBool(restoreIgnoreErrorsFlag)
	body.SetIgnoreErrors(ignore)

	req := new(control.RestoreShardRequest)
	req.SetBody(body)

	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

	var resp *control.RestoreShardResponse
	var err error
	err = cli.ExecRaw(func(client *client.Client) error {
		resp, err = control.RestoreShard(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Shard has been restored successfully.")
}

func initControlRestoreShardCmd() {
	initControlFlags(restoreShardCmd)

	flags := restoreShardCmd.Flags()
	flags.String(shardIDFlag, "", "Shard ID in base58 encoding")
	flags.String(restoreFilepathFlag, "", "File to read objects from")
	flags.Bool(restoreIgnoreErrorsFlag, false, "Skip invalid/unreadable objects")

	_ = restoreShardCmd.MarkFlagRequired(shardIDFlag)
	_ = restoreShardCmd.MarkFlagRequired(restoreFilepathFlag)
	_ = restoreShardCmd.MarkFlagRequired(controlRPC)
}
