package storage

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var storageStatusObjCMD = &cobra.Command{
	Use:   "status",
	Short: "Get object from the NeoFS node's storage snapshot",
	Long:  "Get object from the NeoFS node's storage snapshot",
	Args:  cobra.NoArgs,
	Run:   statusObject,
}

func init() {
	common.AddAddressFlag(storageStatusObjCMD, &vAddress)
	common.AddConfigFileFlag(storageStatusObjCMD, &vConfig)
}

func statusObject(cmd *cobra.Command, _ []string) {
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	common.ExitOnErr(cmd, common.Errf("invalid address argument: %w", err))

	storage := openEngine(cmd)
	defer storage.Close()
	status, err := storage.ObjectStatus(addr)
	common.ExitOnErr(cmd, common.Errf("could not fetch object: %w", err))

	common.PrintStorageObjectStatus(cmd, status)
}
