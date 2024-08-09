package control

import (
	"fmt"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

const objectFlag = "object"

var objectCmd = &cobra.Command{
	Use:   "object",
	Short: "Direct object operations with storage engine",
}

var objectStatusCmd = &cobra.Command{
	Use:          "status",
	Short:        "Check current object status",
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE:         objectStatus,
}

func objectStatus(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.Get(cmd)
	addressRaw, err := cmd.Flags().GetString(objectFlag)
	if err != nil {
		return fmt.Errorf("reading %s flag: %w", objectFlag, err)
	}

	var sdkAddr oid.Address
	err = sdkAddr.DecodeString(addressRaw)
	if err != nil {
		return fmt.Errorf("validating address (%s): %w", addressRaw, err)
	}

	var resp *control.ObjectStatusResponse
	req := &control.ObjectStatusRequest{
		Body: &control.ObjectStatusRequest_Body{
			ObjectAddress: addressRaw,
		},
	}
	signRequest(cmd, pk, req)

	cli := getClient(ctx, cmd)

	err = cli.ExecRaw(func(client *rawclient.Client) error {
		resp, err = control.ObjectStatus(client, req)
		return err
	})
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	verifyResponse(cmd, resp.GetSignature(), resp.GetBody())

	shards := resp.GetBody().GetShards()
	if len(shards) == 0 {
		cmd.Println("<empty response>")
		return nil
	}

	for _, shard := range shards {
		cmd.Printf("Shard ID: %s\n", shard.ShardId)
		storages := shard.GetStorages()
		if len(storages) == 0 {
			cmd.Println("\t<empty response>")
			continue
		}

		for _, storage := range storages {
			cmd.Printf("\t%s: %s\n", storage.Type, storage.Status)
		}
	}

	return nil
}

func initObjectStatusFlags() {
	initControlFlags(objectStatusCmd)

	flags := objectStatusCmd.Flags()
	flags.String(objectFlag, "", "Object address")
}
