package meta

import (
	"fmt"
	"slices"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal"
	blobstorcommon "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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

	err = db.Init(blobstorcommon.ID{})
	if err != nil {
		return fmt.Errorf("can't init metabase: %w", err)
	}

	var (
		cnr  cid.ID
		oids []oid.ID
	)

	var delGroup = func(cnr cid.ID, oids []oid.ID) error {
		res, _, err := db.Delete(cnr, oids)
		if err != nil {
			return fmt.Errorf("can't remove objects: %w", err)
		}
		for _, id := range res {
			cmd.Println("Removed:", id.String())
		}
		return nil
	}

	slices.SortFunc(addrs, oid.Address.Compare)

	for i := range addrs {
		if cnr != addrs[i].Container() {
			if len(oids) != 0 {
				err = delGroup(cnr, oids)
				if err != nil {
					return err
				}
			}
			cnr = addrs[i].Container()
			oids = oids[:0]
		}
		oids = append(oids, addrs[i].Object())
	}
	return delGroup(cnr, oids)
}
