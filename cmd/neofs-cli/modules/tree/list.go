package tree

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "Get tree IDs",
	Args:  cobra.NoArgs,
	RunE:  list,
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		commonflags.Bind(cmd)
	},
}

func initListCmd() {
	commonflags.Init(listCmd)

	ff := listCmd.Flags()
	ff.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = listCmd.MarkFlagRequired(commonflags.CIDFlag)

	_ = cobra.MarkFlagRequired(ff, commonflags.RPC)
}

func list(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}
	cidString, _ := cmd.Flags().GetString(commonflags.CIDFlag)

	var cnr cid.ID
	err = cnr.DecodeString(cidString)
	if err != nil {
		return fmt.Errorf("decode container ID string: %w", err)
	}

	cli, err := _client()
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}

	req := &tree.TreeListRequest{
		Body: &tree.TreeListRequest_Body{
			ContainerId: cnr[:],
		},
	}

	if err := tree.SignMessage(req, pk); err != nil {
		return fmt.Errorf("message signing: %w", err)
	}

	resp, err := cli.TreeList(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc call: %w", err)
	}

	for _, treeID := range resp.GetBody().GetIds() {
		cmd.Println(treeID)
	}

	return nil
}
