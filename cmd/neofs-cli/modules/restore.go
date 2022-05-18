package cmd

import (
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
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
	Run:   restoreShard,
}

func restoreShard(cmd *cobra.Command, _ []string) {
	key, err := getKeyNoGenerate()
	common.ExitOnErr(cmd, "", err)

	body := new(control.RestoreShardRequest_Body)

	rawID, err := base58.Decode(shardID)
	common.ExitOnErr(cmd, "incorrect shard ID encoding: %w", err)
	body.SetShardID(rawID)

	p, _ := cmd.Flags().GetString(restoreFilepathFlag)
	body.SetFilepath(p)

	ignore, _ := cmd.Flags().GetBool(restoreIgnoreErrorsFlag)
	body.SetIgnoreErrors(ignore)

	req := new(control.RestoreShardRequest)
	req.SetBody(body)

	err = controlSvc.SignMessage(key, req)
	common.ExitOnErr(cmd, "could not sign request: %w", err)

	cli, err := getControlSDKClient(key)
	common.ExitOnErr(cmd, "", err)

	var resp *control.RestoreShardResponse
	err = cli.ExecRaw(func(client *client.Client) error {
		resp, err = control.RestoreShard(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Shard has been restored successfully.")
}

func initControlRestoreShardCmd() {
	commonflags.InitWithoutRPC(restoreShardCmd)

	flags := restoreShardCmd.Flags()
	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringVarP(&shardID, shardIDFlag, "", "", "Shard ID in base58 encoding")
	flags.String(restoreFilepathFlag, "", "File to read objects from")
	flags.Bool(restoreIgnoreErrorsFlag, false, "Skip invalid/unreadable objects")

	_ = restoreShardCmd.MarkFlagRequired(shardIDFlag)
	_ = restoreShardCmd.MarkFlagRequired(restoreFilepathFlag)
	_ = restoreShardCmd.MarkFlagRequired(controlRPC)
}
