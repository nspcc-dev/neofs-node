package meta

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var removeCMD = &cobra.Command{
	Use:   "remove",
	Short: "Remove objects from metabase",
	Long:  "Remove objects from metabase",
	Args:  cobra.NoArgs,
	RunE:  removeFunc,
}

var vAddresses []string

const addressesFlagName = "addresses"

func init() {
	common.AddComponentPathFlag(removeCMD, &vPath)
	removeCMD.Flags().StringSliceVar(&vAddresses, addressesFlagName, nil, "Object addresses to remove separated by comma")
	_ = removeCMD.MarkFlagRequired(addressesFlagName)
}

func removeFunc(cmd *cobra.Command, _ []string) error {
	addrs := make([]oid.Address, 0, len(vAddresses))
	for _, addrStr := range vAddresses {
		var addr oid.Address
		err := addr.DecodeString(addrStr)
		if err != nil {
			return fmt.Errorf("invalid address argument: %w", err)
		}
		addrs = append(addrs, addr)
	}

	db, err := openMeta(false)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Init()
	if err != nil {
		return fmt.Errorf("can't init metabase: %w", err)
	}

	res, err := db.Delete(addrs)
	if err != nil {
		return fmt.Errorf("can't remove objects: %w", err)
	}

	for _, r := range res.RemovedObjects {
		cmd.Println("Removed:", r.Address.String())
	}
	return nil
}
