package fstree

import (
	"errors"
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lancet/internal"
	blobstorcommon "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var removeCMD = &cobra.Command{
	Use:   "remove",
	Short: "Remove objects",
	Long:  `Remove objects from FSTree.`,
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

	fst, err := openFSTree(false)
	if err != nil {
		return err
	}
	defer fst.Close()

	err = fst.Init(blobstorcommon.ID{})
	if err != nil {
		return fmt.Errorf("failed to init FSTree: %w", err)
	}

	var errs []error
	for _, addr := range addrs {
		err = fst.Delete(addr)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to remove object %s: %w", addr, err))
			continue
		}

		cmd.Println("Removed:", addr)
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to remove %d object(s): %w", len(errs), errors.Join(errs...))
	}
	return nil
}
