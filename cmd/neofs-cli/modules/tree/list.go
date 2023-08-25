package tree

import (
	"crypto/sha256"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
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
	Run:   list,
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

func list(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.GetOrGenerate(cmd)
	cidString, _ := cmd.Flags().GetString(commonflags.CIDFlag)

	var cnr cid.ID
	err := cnr.DecodeString(cidString)
	common.ExitOnErr(cmd, "decode container ID string: %w", err)

	cli, err := _client(ctx)
	common.ExitOnErr(cmd, "client: %w", err)

	rawCID := make([]byte, sha256.Size)
	cnr.Encode(rawCID)

	req := &tree.TreeListRequest{
		Body: &tree.TreeListRequest_Body{
			ContainerId: rawCID,
		},
	}

	common.ExitOnErr(cmd, "message signing: %w", tree.SignMessage(req, pk))

	resp, err := cli.TreeList(ctx, req)
	common.ExitOnErr(cmd, "rpc call: %w", err)

	for _, treeID := range resp.GetBody().GetIds() {
		cmd.Println(treeID)
	}
}
