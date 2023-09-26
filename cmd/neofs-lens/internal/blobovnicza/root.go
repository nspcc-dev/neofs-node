package blobovnicza

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/spf13/cobra"
)

var (
	vAddress     string
	vPath        string
	vOut         string
	vPayloadOnly bool
)

// Root contains `blobovnicza` command definition.
var Root = &cobra.Command{
	Use:   "blobovnicza",
	Short: "Operations with a blobovnicza",
}

func init() {
	Root.AddCommand(listCMD, inspectCMD, getCMD)
}

func openBlobovnicza(cmd *cobra.Command) *blobovnicza.Blobovnicza {
	blz := blobovnicza.New(
		blobovnicza.WithPath(vPath),
		blobovnicza.WithReadOnly(true),
	)
	common.ExitOnErr(cmd, blz.Open())

	return blz
}
