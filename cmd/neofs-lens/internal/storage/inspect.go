package storage

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var storageInspectObjCMD = &cobra.Command{
	Use:   "inspect",
	Short: "Get object from the NeoFS node's storage snapshot",
	Long:  "Get object from the NeoFS node's storage snapshot",
	Args:  cobra.NoArgs,
	Run:   inspectObject,
}

func init() {
	common.AddAddressFlag(storageInspectObjCMD, &vAddress)
	common.AddOutputFileFlag(storageInspectObjCMD, &vOut)
	common.AddConfigFileFlag(storageInspectObjCMD, &vConfig)
}

func inspectObject(cmd *cobra.Command, _ []string) {
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	common.ExitOnErr(cmd, common.Errf("invalid address argument: %w", err))

	storage := openEngine(cmd)
	defer storage.Close()

	obj, err := engine.Get(storage, addr)
	common.ExitOnErr(cmd, common.Errf("could not fetch object: %w", err))

	common.PrintObjectHeader(cmd, *obj)
	data, err := obj.Marshal()
	common.ExitOnErr(cmd, common.Errf("could not marshal object: %w", err))
	common.WriteObjectToFile(cmd, vOut, data)
}
