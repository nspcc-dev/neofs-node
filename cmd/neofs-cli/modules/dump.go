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
	dumpFilepathFlag     = "path"
	dumpIgnoreErrorsFlag = "no-errors"
)

var dumpShardCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump objects from shard",
	Long:  "Dump objects from shard to a file",
	Run:   dumpShard,
}

func dumpShard(cmd *cobra.Command, _ []string) {
	key, err := getKeyNoGenerate()
	common.ExitOnErr(cmd, "", err)

	body := new(control.DumpShardRequest_Body)

	rawID, err := base58.Decode(shardID)
	common.ExitOnErr(cmd, "incorrect shard ID encoding: %w", err)
	body.SetShardID(rawID)

	p, _ := cmd.Flags().GetString(dumpFilepathFlag)
	body.SetFilepath(p)

	ignore, _ := cmd.Flags().GetBool(dumpIgnoreErrorsFlag)
	body.SetIgnoreErrors(ignore)

	req := new(control.DumpShardRequest)
	req.SetBody(body)

	err = controlSvc.SignMessage(key, req)
	common.ExitOnErr(cmd, "could not sign request: %w", err)

	cli, err := getControlSDKClient(key)
	common.ExitOnErr(cmd, "", err)

	var resp *control.DumpShardResponse
	err = cli.ExecRaw(func(client *client.Client) error {
		resp, err = control.DumpShard(client, req)
		return err
	})
	common.ExitOnErr(cmd, "rpc error: %w", err)

	verifyResponseControl(cmd, resp.GetSignature(), resp.GetBody())

	cmd.Println("Shard has been dumped successfully.")
}

func initControlDumpShardCmd() {
	commonflags.InitWithoutRPC(dumpShardCmd)

	flags := dumpShardCmd.Flags()
	flags.String(controlRPC, controlRPCDefault, controlRPCUsage)
	flags.StringVarP(&shardID, shardIDFlag, "", "", "Shard ID in base58 encoding")
	flags.String(dumpFilepathFlag, "", "File to write objects to")
	flags.Bool(dumpIgnoreErrorsFlag, false, "Skip invalid/unreadable objects")

	_ = dumpShardCmd.MarkFlagRequired(shardIDFlag)
	_ = dumpShardCmd.MarkFlagRequired(dumpFilepathFlag)
	_ = dumpShardCmd.MarkFlagRequired(controlRPC)
}
