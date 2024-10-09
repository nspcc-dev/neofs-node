package fstree

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/spf13/cobra"
)

var cleanupCMD = &cobra.Command{
	Use:   "cleanup-tmp",
	Short: "Clean up tmp files in FSTree",
	Long:  "Clean up temporary unused files in FSTree (forcibly interrupted data writes can leave them)",
	Args:  cobra.NoArgs,
	RunE:  cleanupFunc,
}

func init() {
	common.AddComponentPathFlag(cleanupCMD, &vPath)
}

func cleanupFunc(cmd *cobra.Command, _ []string) error {
	fst, err := openFSTree()
	if err != nil {
		return err
	}
	defer fst.Close()

	cmd.Println("Cleaning up tmp files in FSTree")

	return fst.CleanUpTmp()
}
