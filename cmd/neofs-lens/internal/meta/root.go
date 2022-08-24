package meta

import (
	"time"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"
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

func openMeta(cmd *cobra.Command) *meta.DB {
	db := meta.New(
		meta.WithPath(vPath),
		meta.WithBoltDBOptions(&bbolt.Options{
			ReadOnly: true,
			Timeout:  100 * time.Millisecond,
		}),
		meta.WithEpochState(epochState{}),
	)
	common.ExitOnErr(cmd, common.Errf("could not open metabase: %w", db.Open(true)))

	return db
}
