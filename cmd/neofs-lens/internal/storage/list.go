package storage

import (
	"errors"
	"io"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/spf13/cobra"
)

var storageListObjsCMD = &cobra.Command{
	Use:   "list",
	Short: "Object listing",
	Long:  `List all objects stored in a blobstor (as registered in metabase).`,
	Args:  cobra.NoArgs,
	Run:   listFunc,
}

func init() {
	common.AddConfigFileFlag(storageListObjsCMD, &vConfig)
}

func listFunc(cmd *cobra.Command, _ []string) {
	// other targets can be supported
	w := cmd.OutOrStderr()

	storage := openEngine(cmd)
	defer storage.Close()

	var p engine.ListWithCursorPrm
	p.WithCount(1024)
	for {
		r, err := storage.ListWithCursor(p)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				return
			}
			common.ExitOnErr(cmd, common.Errf("Storage iterator failure: %w", err))
		}
		var addrs = r.AddressList()
		for _, at := range addrs {
			_, err = io.WriteString(w, at.Address.String()+"\n")
			common.ExitOnErr(cmd, common.Errf("print failure: %w", err))
		}
		p.WithCursor(r.Cursor())
	}
}
