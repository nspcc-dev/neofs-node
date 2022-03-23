package token

import (
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "token",
	Short: "Operations with bearer token",
}

func init() {
	Cmd.AddCommand(createCmd)
}
