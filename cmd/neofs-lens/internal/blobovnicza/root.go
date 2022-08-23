package blobovnicza

import "github.com/spf13/cobra"

var (
	vAddress string
	vPath    string
	vOut     string
)

// Root contains `blobovnicza` command definition.
var Root = &cobra.Command{
	Use:   "blobovnicza",
	Short: "Operations with a blobovnicza",
}

func init() {
	Root.AddCommand(listCMD, inspectCMD)
}
