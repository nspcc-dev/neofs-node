package writecache

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
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

func openWC(cmd *cobra.Command) *bbolt.DB {
	db, err := writecache.OpenDB(vPath, true)
	common.ExitOnErr(cmd, common.Errf("could not open write-cache db: %w", err))

	return db
}
