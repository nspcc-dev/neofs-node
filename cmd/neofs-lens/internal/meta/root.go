package meta

import (
	"github.com/spf13/cobra"
)

var (
	vAddress string
	vPath    string
)

type epochState struct{}

func (s epochState) CurrentEpoch() uint64 {
	return 0
}

// Root contains `meta` command definition.
var Root = &cobra.Command{
	Use:   "meta",
	Short: "Operations with a metabase",
}

func init() {
	Root.AddCommand(
		inspectCMD,
		listGraveyardCMD,
		listGarbageCMD,
	)
}
