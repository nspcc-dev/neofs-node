package control

import (
	"github.com/spf13/cobra"
)

var networkCmd = &cobra.Command{
	Use:   "network",
	Short: "Request network changes to inner ring",
}

func initControlNetworkCmd() {
	networkCmd.AddCommand(listNetworkCmd)
	networkCmd.AddCommand(networkEpochTickCmd)

	initControlNetworkListCmd()
	initNetworkEpochTickCmd()
}
