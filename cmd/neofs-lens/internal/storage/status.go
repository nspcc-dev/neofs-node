package storage

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var storageStatusObjCMD = &cobra.Command{
	Use:   "status",
	Short: "Get object from the NeoFS node's storage snapshot",
	Long:  "Get object from the NeoFS node's storage snapshot",
	Args:  cobra.NoArgs,
	RunE:  statusObject,
}

func init() {
	common.AddAddressFlag(storageStatusObjCMD, &vAddress)
	common.AddConfigFileFlag(storageStatusObjCMD, &vConfig)
}

func statusObject(cmd *cobra.Command, _ []string) error {
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	if err != nil {
		return fmt.Errorf("invalid address argument: %w", err)
	}

	storage, err := openEngine()
	if err != nil {
		return err
	}
	defer storage.Close()
	status, err := storage.ObjectStatus(addr)
	if err != nil {
		return fmt.Errorf("could not fetch object: %w", err)
	}

	common.PrintStorageObjectStatus(cmd, status)
	return nil
}
