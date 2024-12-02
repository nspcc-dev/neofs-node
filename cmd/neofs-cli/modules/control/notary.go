package control

import (
	"github.com/spf13/cobra"
)

var notaryCmd = &cobra.Command{
	Use:   "notary",
	Short: "Commands with notary request with alphabet key of inner ring node",
}

func initControlNotaryCmd() {
	notaryCmd.AddCommand(listNotaryCmd)
	notaryCmd.AddCommand(notaryRequestCmd)
	notaryCmd.AddCommand(notarySignCmd)

	initControlNotaryListCmd()
	initControlNotaryRequestCmd()
	initControlNotarySignCmd()
}
