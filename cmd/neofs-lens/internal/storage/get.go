package storage

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var storageGetObjCMD = &cobra.Command{
	Use:   "get",
	Short: "Get object from the NeoFS node's storage snapshot",
	Long:  "Get object from the NeoFS node's storage snapshot",
	Args:  cobra.NoArgs,
	RunE:  getFunc,
}

func init() {
	common.AddAddressFlag(storageGetObjCMD, &vAddress)
	common.AddOutputFileFlag(storageGetObjCMD, &vOut)
	common.AddConfigFileFlag(storageGetObjCMD, &vConfig)
	common.AddPayloadOnlyFlag(storageGetObjCMD, &vPayloadOnly)
}

func getFunc(cmd *cobra.Command, _ []string) error {
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

	obj, err := storage.Get(addr)
	if err != nil {
		return fmt.Errorf("could not fetch object: %w", err)
	}

	common.PrintObjectHeader(cmd, *obj)
	if vPayloadOnly {
		if err := common.WriteObjectToFile(cmd, vOut, obj.Payload(), true); err != nil {
			return err
		}
		return nil
	}
	data := obj.Marshal()
	if err := common.WriteObjectToFile(cmd, vOut, data, false); err != nil {
		return err
	}

	return nil
}
