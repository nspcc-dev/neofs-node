package meta

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/spf13/cobra"
)

var listGarbageCMD = &cobra.Command{
	Use:   "list-garbage",
	Short: "Garbage listing",
	Long:  `List all the objects that have received GC Mark.`,
	Args:  cobra.NoArgs,
	RunE:  listGarbageFunc,
}

func init() {
	common.AddComponentPathFlag(listGarbageCMD, &vPath)
}

func listGarbageFunc(cmd *cobra.Command, _ []string) error {
	db, err := openMeta(true)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.IterateOverGarbage(func(garbageObject meta.GarbageObject) error {
		cmd.Println(garbageObject.Address().EncodeToString())
		return nil
	}, nil)
	if err != nil {
		return fmt.Errorf("could not iterate over garbage bucket: %w", err)
	}

	return nil
}
