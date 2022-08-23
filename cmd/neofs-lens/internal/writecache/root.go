package writecache

import "github.com/spf13/cobra"

var (
	vAddress string
	vPath    string
	vOut     string
)

// Root contains `write-cache` command definition.
var Root = &cobra.Command{
	Use:   "write-cache",
	Short: "Operations with write-cache",
}

func init() {
	Root.AddCommand(listCMD, inspectCMD)
}
