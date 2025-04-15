package control

import (
	"fmt"

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
	RunE:  dumpShard,
}

func dumpShard(cmd *cobra.Command, _ []string) error {
	pk, err := key.Get(cmd)
	if err != nil {
		return err
	}

	body := new(control.DumpShardRequest_Body)
	id, err := getShardID(cmd)
	if err != nil {
		return err
	}
	body.SetShardID(id)

	p, _ := cmd.Flags().GetString(dumpFilepathFlag)
	body.SetFilepath(p)

	ignore, _ := cmd.Flags().GetBool(dumpIgnoreErrorsFlag)
	body.SetIgnoreErrors(ignore)

	req := new(control.DumpShardRequest)
	req.SetBody(body)

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

	resp, err := cli.DumpShard(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	if err = verifyResponse(resp.GetSignature(), resp.GetBody()); err != nil {
		return err
	}

	cmd.Println("Shard has been dumped successfully.")
	return nil
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
