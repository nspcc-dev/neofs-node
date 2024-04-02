package object

import (
	"fmt"

	"github.com/spf13/cobra"
)

var vPath string

// Root defines root command for operations with NeoFS objects.
var Root = &cobra.Command{
	Use:   "object",
	Short: "NeoFS object inspection",
}

const filePathFlagKey = "file"

func init() {
	pFlags := Root.PersistentFlags()
	pFlags.StringVar(&vPath, filePathFlagKey, "", "Path to file with NeoFS object")

	err := cobra.MarkFlagFilename(pFlags, filePathFlagKey)
	if err != nil {
		panic(fmt.Errorf("MarkFlagFilename for %s flag: %w", filePathFlagKey, err))
	}
	err = cobra.MarkFlagRequired(pFlags, filePathFlagKey)
	if err != nil {
		panic(fmt.Errorf("MarkFlagRequired for %s flag: %w", filePathFlagKey, err))
	}

	Root.AddCommand(linkCMD)
}
