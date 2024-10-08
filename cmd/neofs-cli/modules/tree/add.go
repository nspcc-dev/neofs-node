package tree

import (
	"fmt"
	"strings"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/pkg/services/tree"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/spf13/cobra"
)

var addCmd = &cobra.Command{
	Use:   "add",
	Short: "Add a node to the tree service",
	RunE:  add,
	Args:  cobra.NoArgs,
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		commonflags.Bind(cmd)
	},
}

func initAddCmd() {
	commonflags.Init(addCmd)
	initCTID(addCmd)

	ff := addCmd.Flags()
	ff.StringSlice(metaFlagKey, nil, "Meta pairs in the form of Key1=[0x]Value1,Key2=[0x]Value2")
	ff.Uint64(parentIDFlagKey, 0, "Parent node ID")

	_ = cobra.MarkFlagRequired(ff, commonflags.RPC)
}

func add(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	var cnr cid.ID
	err = cnr.DecodeString(cmd.Flag(commonflags.CIDFlag).Value.String())
	if err != nil {
		return fmt.Errorf("decode container ID string: %w", err)
	}

	tid, _ := cmd.Flags().GetString(treeIDFlagKey)
	pid, _ := cmd.Flags().GetUint64(parentIDFlagKey)

	meta, err := parseMeta(cmd)
	if err != nil {
		return fmt.Errorf("meta data parsing: %w", err)
	}

	cli, err := _client()
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}

	req := new(tree.AddRequest)
	req.Body = &tree.AddRequest_Body{
		ContainerId: cnr[:],
		TreeId:      tid,
		ParentId:    pid,
		Meta:        meta,
		BearerToken: nil, // TODO: #1891 add token handling
	}

	if err := tree.SignMessage(req, pk); err != nil {
		return fmt.Errorf("message signing: %w", err)
	}

	resp, err := cli.Add(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc call: %w", err)
	}

	cmd.Println("Node ID: ", resp.Body.NodeId)

	return nil
}

func parseMeta(cmd *cobra.Command) ([]*tree.KeyValue, error) {
	raws, _ := cmd.Flags().GetStringSlice(metaFlagKey)
	if len(raws) == 0 {
		return nil, nil
	}

	pairs := make([]*tree.KeyValue, 0, len(raws))
	for i := range raws {
		kv := strings.SplitN(raws[i], "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid meta pair format: %s", raws[i])
		}

		var pair tree.KeyValue
		pair.Key = kv[0]
		pair.Value = []byte(kv[1])

		pairs = append(pairs, &pair)
	}

	return pairs, nil
}
