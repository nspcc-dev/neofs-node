package basic

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "basic",
	Short: "Operations with Basic Access Control Lists",
}

func init() {
	Cmd.AddCommand(printACLCmd)
}
