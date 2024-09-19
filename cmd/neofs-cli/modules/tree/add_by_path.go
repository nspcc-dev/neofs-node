package tree

import (
	"fmt"
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
	RunE:  addByPath,
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

func addByPath(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	cidRaw, _ := cmd.Flags().GetString(commonflags.CIDFlag)

	var cnr cid.ID
	err = cnr.DecodeString(cidRaw)
	if err != nil {
		return fmt.Errorf("decode container ID string: %w", err)
	}

	tid, _ := cmd.Flags().GetString(treeIDFlagKey)

	cli, err := _client()
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}

	meta, err := parseMeta(cmd)
	if err != nil {
		return fmt.Errorf("meta data parsing: %w", err)
	}

	path, _ := cmd.Flags().GetString(pathFlagKey)
	//pAttr, _ := cmd.Flags().GetString(pathAttributeFlagKey)

	req := new(tree.AddByPathRequest)
	req.Body = &tree.AddByPathRequest_Body{
		ContainerId:   cnr[:],
		TreeId:        tid,
		PathAttribute: object.AttributeFileName,
		//PathAttribute: pAttr,
		Path:        strings.Split(path, "/"),
		Meta:        meta,
		BearerToken: nil, // TODO: #1891 add token handling
	}

	if err := tree.SignMessage(req, pk); err != nil {
		return fmt.Errorf("message signing: %w", err)
	}

	resp, err := cli.AddByPath(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc call: %w", err)
	}

	cmd.Printf("Parent ID: %d\n", resp.GetBody().GetParentId())

	nn := resp.GetBody().GetNodes()
	if len(nn) == 0 {
		common.PrintVerbose(cmd, "No new nodes were created")
		return nil
	}

	cmd.Println("Created nodes:")
	for _, node := range resp.GetBody().GetNodes() {
		cmd.Printf("\t%d\n", node)
	}

	return nil
}
