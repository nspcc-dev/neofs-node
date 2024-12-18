package control

import (
	"fmt"

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
	RunE:  restoreShard,
}

func restoreShard(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	body := new(control.RestoreShardRequest_Body)
	id, err := getShardID(cmd)
	if err != nil {
		return err
	}
	body.SetShardID(id)

	p, _ := cmd.Flags().GetString(restoreFilepathFlag)
	body.SetFilepath(p)

	ignore, _ := cmd.Flags().GetBool(restoreIgnoreErrorsFlag)
	body.SetIgnoreErrors(ignore)

	req := new(control.RestoreShardRequest)
	req.SetBody(body)

	err = signRequest(pk, req)
	if err != nil {
		return err
	}

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	resp, err := cli.RestoreShard(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	cmd.Println("Shard has been restored successfully.")
	return nil
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
