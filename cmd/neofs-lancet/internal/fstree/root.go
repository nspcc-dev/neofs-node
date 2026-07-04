package fstree

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/spf13/cobra"
)

var (
	vAddress     string
	vPath        string
	vOut         string
	vPayloadOnly bool
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
	Root.AddCommand(removeCMD)
}

// openFSTree opens and returns fstree.FSTree located in vPath.
func openFSTree(readOnly bool) (*fstree.FSTree, error) {
	fst := fstree.New(
		fstree.WithPath(vPath),
		fstree.WithPerm(0600),
	)

	err := fst.Open(readOnly)
	if err != nil {
		return nil, fmt.Errorf("failed to open FSTree: %w", err)
	}

	return fst, nil
}
