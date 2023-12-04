package storage

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var storageGetObjCMD = &cobra.Command{
	Use:   "get",
	Short: "Get object from the NeoFS node's storage snapshot",
	Long:  "Get object from the NeoFS node's storage snapshot",
	Args:  cobra.NoArgs,
	Run:   getFunc,
}

func init() {
	common.AddAddressFlag(storageGetObjCMD, &vAddress)
	common.AddOutputFileFlag(storageGetObjCMD, &vOut)
	common.AddConfigFileFlag(storageGetObjCMD, &vConfig)
	common.AddPayloadOnlyFlag(storageGetObjCMD, &vPayloadOnly)
}

func getFunc(cmd *cobra.Command, _ []string) {
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	common.ExitOnErr(cmd, common.Errf("invalid address argument: %w", err))

	storage := openEngine(cmd)
	defer storage.Close()

	obj, err := engine.Get(storage, addr)
	common.ExitOnErr(cmd, common.Errf("could not fetch object: %w", err))

	common.PrintObjectHeader(cmd, *obj)
	if vPayloadOnly {
		common.WriteObjectToFile(cmd, vOut, obj.Payload(), true)
		return
	}
	data, err := obj.Marshal()
	common.ExitOnErr(cmd, common.Errf("could not marshal object: %w", err))
	common.WriteObjectToFile(cmd, vOut, data, false)
}
