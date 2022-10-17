package util

import (
	"github.com/spf13/cobra"
)

// locode section.
var locodeCmd = &cobra.Command{
	Use:   "locode",
	Short: "Working with NeoFS UN/LOCODE database",
}

func initLocodeCmd() {
	locodeCmd.AddCommand(locodeGenerateCmd, locodeInfoCmd)

	initUtilLocodeInfoCmd()
	initUtilLocodeGenerateCmd()
}
