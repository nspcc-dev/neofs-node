package meta

import (
	"time"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"
)

var listGarbageCMD = &cobra.Command{
	Use:   "list-garbage",
	Short: "Garbage listing",
	Long:  `List all the objects that have received GC Mark.`,
	Run:   listGarbageFunc,
}

func init() {
	common.AddComponentPathFlag(listGarbageCMD, &vPath)
}

func listGarbageFunc(cmd *cobra.Command, _ []string) {
	db := meta.New(
		meta.WithPath(vPath),
		meta.WithBoltDBOptions(&bbolt.Options{
			ReadOnly: true,
			Timeout:  100 * time.Millisecond,
		}),
		meta.WithEpochState(epochState{}),
	)

	common.ExitOnErr(cmd, common.Errf("could not open metabase: %w", db.Open(true)))

	var garbPrm meta.GarbageIterationPrm
	garbPrm.SetHandler(
		func(garbageObject meta.GarbageObject) error {
			cmd.Println(garbageObject.Address().EncodeToString())
			return nil
		})

	err := db.IterateOverGarbage(garbPrm)
	common.ExitOnErr(cmd, common.Errf("could not iterate over garbage bucket: %w", err))
}
