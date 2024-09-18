package storage

import (
	"errors"
	"fmt"
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
	RunE:  listFunc,
}

func init() {
	common.AddConfigFileFlag(storageListObjsCMD, &vConfig)
}

func listFunc(cmd *cobra.Command, _ []string) error {
	// other targets can be supported
	w := cmd.OutOrStderr()

	storage, err := openEngine()
	if err != nil {
		return err
	}
	defer storage.Close()

	var p engine.ListWithCursorPrm
	p.WithCount(1024)
	for {
		r, err := storage.ListWithCursor(p)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("Storage iterator failure: %w", err)
			}
		}
		var addrs = r.AddressList()
		for _, at := range addrs {
			_, err = io.WriteString(w, at.Address.String()+"\n")
			if err != nil {
				return fmt.Errorf("print failure: %w", err)
			}
		}
		p.WithCursor(r.Cursor())
	}
}
