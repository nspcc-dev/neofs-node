package bearer

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "bearer",
	Short: "Operations with bearer token",
}

func init() {
	Cmd.AddCommand(createCmd)
	Cmd.AddCommand(printCmd)
}
