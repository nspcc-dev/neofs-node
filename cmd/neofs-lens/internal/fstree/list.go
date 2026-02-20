package fstree

import (
	"fmt"
	"io"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var listCMD = &cobra.Command{
	Use:   "list",
	Short: "Object listing",
	Long:  `List all objects stored in FSTree.`,
	Args:  cobra.NoArgs,
	RunE:  listFunc,
}

func init() {
	common.AddComponentPathFlag(listCMD, &vPath)
	AddDepthFlag(listCMD, &vDepth)
}

func listFunc(cmd *cobra.Command, _ []string) error {
	// other targets can be supported
	w := cmd.OutOrStderr()

	wAddr := func(addr oid.Address) error {
		_, err := io.WriteString(w, addr.EncodeToString()+"\n")
		return err
	}

	fst, err := openFSTree()
	if err != nil {
		return err
	}
	defer fst.Close()

	err = fst.IterateAddresses(wAddr, true)
	if err != nil {
		return fmt.Errorf("fstree iterator failure: %w", err)
	}
	return nil
}
