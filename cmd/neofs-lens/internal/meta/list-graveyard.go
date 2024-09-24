package meta

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/spf13/cobra"
)

var listGraveyardCMD = &cobra.Command{
	Use:   "list-graveyard",
	Short: "Graveyard listing",
	Long:  `List all the objects that have been covered with a Tomb Stone.`,
	Args:  cobra.NoArgs,
	RunE:  listGraveyardFunc,
}

func init() {
	common.AddComponentPathFlag(listGraveyardCMD, &vPath)
}

func listGraveyardFunc(cmd *cobra.Command, _ []string) error {
	db, err := openMeta(true)
	if err != nil {
		return err
	}
	defer db.Close()

	var gravePrm meta.GraveyardIterationPrm
	gravePrm.SetHandler(
		func(tsObj meta.TombstonedObject) error {
			cmd.Printf(
				"Object: %s\nTS: %s (TS expiration: %d)\n",
				tsObj.Address().EncodeToString(),
				tsObj.Tombstone().EncodeToString(),
				tsObj.TombstoneExpiration(),
			)

			return nil
		})

	err = db.IterateOverGraveyard(gravePrm)
	if err != nil {
		return fmt.Errorf("could not iterate over graveyard bucket: %w", err)
	}

	return nil
}
