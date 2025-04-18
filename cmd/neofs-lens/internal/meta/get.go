package meta

import (
	"errors"
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var getCMD = &cobra.Command{
	Use:   "get",
	Short: "Object inspection",
	Long:  `Get specific object from a metabase.`,
	Args:  cobra.NoArgs,
	RunE:  getFunc,
}

func init() {
	common.AddAddressFlag(getCMD, &vAddress)
	common.AddComponentPathFlag(getCMD, &vPath)
}

func getFunc(cmd *cobra.Command, _ []string) error {
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	if err != nil {
		return fmt.Errorf("invalid address argument: %w", err)
	}

	db, err := openMeta(true)
	if err != nil {
		return err
	}
	defer db.Close()

	siErr := new(object.SplitInfoError)

	obj, err := db.Get(addr, true)
	if errors.As(err, &siErr) {
		link := siErr.SplitInfo().GetLink()
		last := siErr.SplitInfo().GetLastPart()

		fmt.Println("Object is split")
		cmd.Println("\tSplitID:", siErr.SplitInfo().SplitID().String())

		if !link.IsZero() {
			cmd.Println("\tLink:", link)
		}
		if !last.IsZero() {
			cmd.Println("\tLast:", last)
		}

		return nil
	}
	if err != nil {
		return fmt.Errorf("could not get object: %w", err)
	}

	common.PrintObjectHeader(cmd, *obj)

	return nil
}
