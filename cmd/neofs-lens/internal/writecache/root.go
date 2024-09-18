package writecache

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"
)

var (
	vAddress     string
	vPath        string
	vOut         string
	vPayloadOnly bool
)

// Root contains `write-cache` command definition.
var Root = &cobra.Command{
	Use:   "write-cache",
	Short: "Operations with write-cache",
}

func init() {
	Root.AddCommand(listCMD)
	Root.AddCommand(getCMD)
}

func openWC() (*bbolt.DB, error) {
	db, err := writecache.OpenDB(vPath, true)
	if err != nil {
		return nil, fmt.Errorf("could not open write-cache db: %w", err)
	}

	return db, nil
}
