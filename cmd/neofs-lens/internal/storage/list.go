package storage

import (
	"errors"
	"fmt"
	"io"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
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

	var (
		addrs  []objectcore.AddressWithAttributes
		cursor *engine.Cursor
	)
	for {
		addrs, cursor, err = storage.ListWithCursor(1024, cursor)
		if err != nil {
			if errors.Is(err, engine.ErrEndOfListing) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("Storage iterator failure: %w", err)
			}
		}
		for _, at := range addrs {
			_, err = io.WriteString(w, at.Address.String()+"\n")
			if err != nil {
				return fmt.Errorf("print failure: %w", err)
			}
		}
	}
}
