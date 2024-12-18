package control

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	controlSvc "github.com/nspcc-dev/neofs-node/pkg/services/control/server"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
)

const (
	synchronizeTreeIDFlag     = "tree-id"
	synchronizeTreeHeightFlag = "height"
)

var synchronizeTreeCmd = &cobra.Command{
	Use:   "synchronize-tree",
	Short: "Synchronize log for the tree",
	Long:  "Synchronize log for the tree in an object tree service.",
	Args:  cobra.NoArgs,
	RunE:  synchronizeTree,
}

func initControlSynchronizeTreeCmd() {
	initControlFlags(synchronizeTreeCmd)

	flags := synchronizeTreeCmd.Flags()
	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.String(synchronizeTreeIDFlag, "", "Tree ID")
	flags.Uint64(synchronizeTreeHeightFlag, 0, "Starting height")
}

func synchronizeTree(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.Get(cmd)

	var cnr cid.ID
	cidStr, _ := cmd.Flags().GetString(commonflags.CIDFlag)
	if err := cnr.DecodeString(cidStr); err != nil {
		return fmt.Errorf("can't decode container ID: %w", err)
	}

	treeID, _ := cmd.Flags().GetString("tree-id")
	if treeID == "" {
		return errors.New("tree ID must not be empty")
	}

	height, _ := cmd.Flags().GetUint64("height")

	req := &control.SynchronizeTreeRequest{
		Body: &control.SynchronizeTreeRequest_Body{
			ContainerId: cnr[:],
			TreeId:      treeID,
			Height:      height,
		},
	}

	err = controlSvc.SignMessage(pk, req)
	if err != nil {
		return fmt.Errorf("could not sign request: %w", err)
	}

	cli, err := getClient(ctx)
	if err != nil {
		return err
	}

	resp, err := cli.SynchronizeTree(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	err = verifyResponse(resp.GetSignature(), resp.GetBody())
	if err != nil {
		return err
	}

	cmd.Println("Tree has been synchronized successfully.")
	return nil
}
