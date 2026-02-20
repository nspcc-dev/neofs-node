package fstree

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/spf13/cobra"
)

var (
	vAddress     string
	vPath        string
	vOut         string
	vPayloadOnly bool
	vDepth       uint64
)

// Root defines root command for operations with FSTree.
var Root = &cobra.Command{
	Use:   "fstree",
	Short: "Operations with FSTree storage subsystem",
}

func init() {
	Root.AddCommand(cleanupCMD)
	Root.AddCommand(getCMD)
	Root.AddCommand(listCMD)
}

func AddDepthFlag(cmd *cobra.Command, depth *uint64) {
	cmd.Flags().Uint64VarP(depth, "depth", "d", 4, "FSTree depth, for write-cache 1 must be used")
}

// openFSTree opens and returns fstree.FSTree located in vPath.
func openFSTree() (*fstree.FSTree, error) {
	fst := fstree.New(
		fstree.WithPath(vPath),
		fstree.WithPerm(0600),
		fstree.WithDepth(vDepth),
	)

	var compressCfg compression.Config

	err := compressCfg.Init()
	if err != nil {
		return nil, fmt.Errorf("failed to init compression config: %w", err)
	}

	fst.SetCompressor(&compressCfg)

	err = fst.Open(false)
	if err != nil {
		return nil, fmt.Errorf("failed to open FSTree: %w", err)
	}

	return fst, nil
}
