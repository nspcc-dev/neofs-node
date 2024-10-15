package control

import (
	"github.com/spf13/cobra"
)

const objectFlag = "object"

var objectCmd = &cobra.Command{
	Use:   "object",
	Short: "Direct object operations with storage engine",
}

func initControlObjectsCmd() {
	objectCmd.AddCommand(listObjectsCmd)
	objectCmd.AddCommand(objectStatusCmd)
	objectCmd.AddCommand(reviveObjectCmd)

	initControlObjectReviveCmd()
	initControlObjectsListCmd()
	initObjectStatusFlags()
}
