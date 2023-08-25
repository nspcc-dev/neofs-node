package tree

import (
	"crypto/sha256"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
)

var getByPathCmd = &cobra.Command{
	Use:   "get-by-path",
	Short: "Get a node by its path",
	Args:  cobra.NoArgs,
	Run:   getByPath,
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		commonflags.Bind(cmd)
	},
}

func initGetByPathCmd() {
	commonflags.Init(getByPathCmd)
	initCTID(getByPathCmd)

	ff := getByPathCmd.Flags()

	// tree service does not allow any attribute except
	// the 'FileName' but that's a limitation of the
	// current implementation, not the rule
	//ff.String(pathAttributeFlagKey, "", "Path attribute")
	ff.String(pathFlagKey, "", "Path to a node")

	ff.Bool(latestOnlyFlagKey, false, "Look only for the latest version of a node")

	_ = cobra.MarkFlagRequired(ff, commonflags.RPC)
}

func getByPath(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.GetOrGenerate(cmd)

	cidRaw, _ := cmd.Flags().GetString(commonflags.CIDFlag)

	var cnr cid.ID
	err := cnr.DecodeString(cidRaw)
	common.ExitOnErr(cmd, "decode container ID string: %w", err)

	tid, _ := cmd.Flags().GetString(treeIDFlagKey)

	cli, err := _client(ctx)
	common.ExitOnErr(cmd, "client: %w", err)

	rawCID := make([]byte, sha256.Size)
	cnr.Encode(rawCID)

	latestOnly, _ := cmd.Flags().GetBool(latestOnlyFlagKey)
	path, _ := cmd.Flags().GetString(pathFlagKey)
	//pAttr, _ := cmd.Flags().GetString(pathAttributeFlagKey)

	req := new(tree.GetNodeByPathRequest)
	req.Body = &tree.GetNodeByPathRequest_Body{
		ContainerId:   rawCID,
		TreeId:        tid,
		PathAttribute: object.AttributeFileName,
		//PathAttribute: pAttr,
		Path:          strings.Split(path, "/"),
		LatestOnly:    latestOnly,
		AllAttributes: true,
		BearerToken:   nil, // TODO: #1891 add token handling
	}

	common.ExitOnErr(cmd, "message signing: %w", tree.SignMessage(req, pk))

	resp, err := cli.GetNodeByPath(ctx, req)
	common.ExitOnErr(cmd, "rpc call: %w", err)

	nn := resp.GetBody().GetNodes()
	if len(nn) == 0 {
		common.PrintVerbose(cmd, "The node is not found")
		return
	}

	for _, n := range nn {
		cmd.Printf("%d:\n", n.GetNodeId())

		cmd.Println("\tParent ID: ", n.GetParentId())
		cmd.Println("\tTimestamp: ", n.GetTimestamp())

		cmd.Println("\tMeta pairs: ")
		for _, kv := range n.GetMeta() {
			cmd.Printf("\t\t%s: %s\n", kv.GetKey(), string(kv.GetValue()))
		}
	}
}
