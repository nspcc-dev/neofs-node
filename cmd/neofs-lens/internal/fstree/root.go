package fstree

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/compression"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/spf13/cobra"
)

var (
	vPath string
)

// Root defines root command for operations with FSTree.
var Root = &cobra.Command{
	Use:   "fstree",
	Short: "Operations with a FSTree",
}

func init() {
	Root.AddCommand(cleanupCMD)
}

// openFSTree open and returns read-only fstree.FSTree located in vPath.
func openFSTree() (*fstree.FSTree, error) {
	fst := fstree.New(
		fstree.WithPath(vPath),
		fstree.WithPerm(0400),
	)

	var compressCfg compression.Config

	err := compressCfg.Init()
	if err != nil {
		return nil, fmt.Errorf("failed to init compression config: %w", err)
	}

	fst.SetCompressor(&compressCfg)

	err = fst.Open(true)
	if err != nil {
		return nil, fmt.Errorf("failed to open FSTree: %w", err)
	}

	return fst, nil
}
