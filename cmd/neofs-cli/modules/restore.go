package cmd

import (
	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	"github.com/nspcc-dev/neofs-sdk-go/util/signature"
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
	exitOnErr(cmd, err)

	body := new(control.RestoreShardRequest_Body)

	rawID, err := base58.Decode(shardID)
	exitOnErr(cmd, errf("incorrect shard ID encoding: %w", err))
	body.SetShardID(rawID)

	p, _ := cmd.Flags().GetString(restoreFilepathFlag)
	body.SetFilepath(p)

	ignore, _ := cmd.Flags().GetBool(restoreIgnoreErrorsFlag)
	body.SetIgnoreErrors(ignore)

	req := new(control.RestoreShardRequest)
	req.SetBody(body)

	err = controlSvc.SignMessage(key, req)
	exitOnErr(cmd, errf("could not sign request: %w", err))

	cli, err := getControlSDKClient(key)
	exitOnErr(cmd, err)

	var resp *control.RestoreShardResponse
	err = cli.ExecRaw(func(client *client.Client) error {
		resp, err = control.RestoreShard(client, req)
		return err
	})
	exitOnErr(cmd, errf("rpc error: %w", err))

	sign := resp.GetSignature()

	err = signature.VerifyDataWithSource(
		resp,
		func() ([]byte, []byte) {
			return sign.GetKey(), sign.GetSign()
		},
	)
	exitOnErr(cmd, errf("invalid response signature: %w", err))

	cmd.Println("Shard has been restored successfully.")
}

func initControlRestoreShardCmd() {
	initCommonFlagsWithoutRPC(restoreShardCmd)

	flags := restoreShardCmd.Flags()
	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringVarP(&shardID, shardIDFlag, "", "", "Shard ID in base58 encoding")
	flags.String(restoreFilepathFlag, "", "File to read objects from")
	flags.Bool(restoreIgnoreErrorsFlag, false, "Skip invalid/unreadable objects")

	_ = restoreShardCmd.MarkFlagRequired(shardIDFlag)
	_ = restoreShardCmd.MarkFlagRequired(restoreFilepathFlag)
	_ = restoreShardCmd.MarkFlagRequired(controlRPC)
}
