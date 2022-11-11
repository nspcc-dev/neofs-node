package extended

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "extended",
	Short: "Operations with Extended Access Control Lists",
}

func init() {
	Cmd.AddCommand(createCmd)
	Cmd.AddCommand(printEACLCmd)
}
