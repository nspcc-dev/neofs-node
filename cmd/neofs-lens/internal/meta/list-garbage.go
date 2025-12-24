package meta

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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
	listGarbageCMD.Flags().StringVar(&vAddress, "container", "", "Container ID")
	_ = listGarbageCMD.MarkFlagRequired("container")
}

func listGarbageFunc(cmd *cobra.Command, _ []string) error {
	var cnr cid.ID

	err := cnr.DecodeString(vAddress)
	if err != nil {
		return fmt.Errorf("invalid container argument: %w", err)
	}

	db, err := openMeta(true)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.IterateOverGarbage(func(id oid.ID) error {
		cmd.Println(oid.NewAddress(cnr, id).String())
		return nil
	}, cnr, oid.ID{})
	if err != nil {
		return fmt.Errorf("could not iterate over garbage bucket: %w", err)
	}

	return nil
}
