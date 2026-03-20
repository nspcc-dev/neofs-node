package common

import (
	"github.com/spf13/cobra"
)

const (
	flagAddress    = "address"
	flagEnginePath = "path"
	flagOutFile    = "out"
	flagConfigFile = "config"
	flagInFile     = "obj"
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

// AddConfigFileFlag adds the config file flag to the passed cobra command.
func AddConfigFileFlag(cmd *cobra.Command, v *string) {
	cmd.Flags().StringVar(v, flagConfigFile, "",
		"Path to file with storage node config")
	_ = cmd.MarkFlagFilename(flagConfigFile)
	_ = cmd.MarkFlagRequired(flagConfigFile)
}

func AddPayloadOnlyFlag(cmd *cobra.Command, v *bool) {
	cmd.Flags().BoolVar(v, "payload-only", false, "Save only object payload")
}

// AddInputPathFile adds the input file with object flag to the passed cobra command.
func AddInputPathFile(cmd *cobra.Command, v *string) {
	cmd.Flags().StringVar(v, flagInFile, "",
		"Path to file with object")
	_ = cmd.MarkFlagFilename(flagInFile)
	_ = cmd.MarkFlagRequired(flagInFile)
}
