package mainchain

import (
	"github.com/spf13/cobra"
)

// RootCmd is the root command for mainchain operations.
var RootCmd = &cobra.Command{
	Use:   "mainchain",
	Short: "Main chain network operations",
}

func init() {
	RootCmd.AddCommand(updateContractCommand)

	initUpdateContractCmd()
}
