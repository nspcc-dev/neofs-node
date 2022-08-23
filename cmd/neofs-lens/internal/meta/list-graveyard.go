package meta

import (
	"time"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"
)

var listGraveyardCMD = &cobra.Command{
	Use:   "list-graveyard",
	Short: "Graveyard listing",
	Long:  `List all the objects that have been covered with a Tomb Stone.`,
	Run:   listGraveyardFunc,
}

func init() {
	common.AddComponentPathFlag(listGraveyardCMD, &vPath)
}

func listGraveyardFunc(cmd *cobra.Command, _ []string) {
	db := meta.New(
		meta.WithPath(vPath),
		meta.WithBoltDBOptions(&bbolt.Options{
			ReadOnly: true,
			Timeout:  100 * time.Millisecond,
		}),
		meta.WithEpochState(epochState{}),
	)

	common.ExitOnErr(cmd, common.Errf("could not open metabase: %w", db.Open(true)))

	var gravePrm meta.GraveyardIterationPrm
	gravePrm.SetHandler(
		func(tsObj meta.TombstonedObject) error {
			cmd.Printf(
				"Object: %s\nTS: %s\n",
				tsObj.Address().EncodeToString(),
				tsObj.Tombstone().EncodeToString(),
			)

			return nil
		})

	err := db.IterateOverGraveyard(gravePrm)
	common.ExitOnErr(cmd, common.Errf("could not iterate over graveyard bucket: %w", err))
}
