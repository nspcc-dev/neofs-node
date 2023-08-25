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

var addByPathCmd = &cobra.Command{
	Use:   "add-by-path",
	Short: "Add a node by the path",
	Args:  cobra.NoArgs,
	Run:   addByPath,
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		commonflags.Bind(cmd)
	},
}

func initAddByPathCmd() {
	commonflags.Init(addByPathCmd)
	initCTID(addByPathCmd)

	ff := addByPathCmd.Flags()

	// tree service does not allow any attribute except
	// the 'FileName' but that's a limitation of the
	// current implementation, not the rule
	//ff.String(pathAttributeFlagKey, "", "Path attribute")
	ff.String(pathFlagKey, "", "Path to a node")
	ff.StringSlice(metaFlagKey, nil, "Meta pairs in the form of Key1=[0x]Value1,Key2=[0x]Value2")

	_ = cobra.MarkFlagRequired(ff, commonflags.RPC)
	_ = cobra.MarkFlagRequired(ff, pathFlagKey)
}

func addByPath(cmd *cobra.Command, _ []string) {
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

	meta, err := parseMeta(cmd)
	common.ExitOnErr(cmd, "meta data parsing: %w", err)

	path, _ := cmd.Flags().GetString(pathFlagKey)
	//pAttr, _ := cmd.Flags().GetString(pathAttributeFlagKey)

	req := new(tree.AddByPathRequest)
	req.Body = &tree.AddByPathRequest_Body{
		ContainerId:   rawCID,
		TreeId:        tid,
		PathAttribute: object.AttributeFileName,
		//PathAttribute: pAttr,
		Path:        strings.Split(path, "/"),
		Meta:        meta,
		BearerToken: nil, // TODO: #1891 add token handling
	}

	common.ExitOnErr(cmd, "message signing: %w", tree.SignMessage(req, pk))

	resp, err := cli.AddByPath(ctx, req)
	common.ExitOnErr(cmd, "rpc call: %w", err)

	cmd.Printf("Parent ID: %d\n", resp.GetBody().GetParentId())

	nn := resp.GetBody().GetNodes()
	if len(nn) == 0 {
		common.PrintVerbose(cmd, "No new nodes were created")
		return
	}

	cmd.Println("Created nodes:")
	for _, node := range resp.GetBody().GetNodes() {
		cmd.Printf("\t%d\n", node)
	}
}
