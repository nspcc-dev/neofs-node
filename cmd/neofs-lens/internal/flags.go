package common

import (
	"github.com/spf13/cobra"
)

const (
	flagAddress    = "address"
	flagEnginePath = "path"
	flagOutFile    = "out"
)

// AddAddressFlag adds the address flag to the passed cobra command.
func AddAddressFlag(cmd *cobra.Command, v *string) {
	cmd.Flags().StringVar(v, flagAddress, "", "Object address")
	_ = cmd.MarkFlagRequired(flagAddress)
}

// AddComponentPathFlag adds the path-to-component flag to the
// passed cobra command.
func AddComponentPathFlag(cmd *cobra.Command, v *string) {
	cmd.Flags().StringVar(v, flagEnginePath, "",
		"Path to storage engine component",
	)
	_ = cmd.MarkFlagFilename(flagEnginePath)
	_ = cmd.MarkFlagRequired(flagEnginePath)
}

// AddOutputFileFlag adds the output file flag to the passed cobra
// command.
func AddOutputFileFlag(cmd *cobra.Command, v *string) {
	cmd.Flags().StringVar(v, flagOutFile, "",
		"File to save object payload")
	_ = cmd.MarkFlagFilename(flagOutFile)
}
