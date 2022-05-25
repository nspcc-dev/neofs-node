package util

import "github.com/spf13/cobra"

var convertCmd = &cobra.Command{
	Use:   "convert",
	Short: "Convert representation of NeoFS structures",
}

func initConvertCmd() {
	convertCmd.AddCommand(convertEACLCmd)

	initConvertEACLCmd()
}
