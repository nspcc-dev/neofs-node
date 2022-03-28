package session

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "session",
	Short: "Operations with session token",
}

func init() {
	Cmd.AddCommand(createCmd)
}
