package control

import (
	"github.com/spf13/cobra"
)

var objectCmd = &cobra.Command{
	Use:   "object",
	Short: "Direct object operations with storage engine",
}

func initControlObjectsCmd() {
	objectCmd.AddCommand(listObjectsCmd)
	objectCmd.AddCommand(objectStatusCmd)

	initControlObjectsListCmd()
	initObjectStatusFlags()
}
