package peapod

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	blobstorcommon "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/spf13/cobra"
)

var getCMD = &cobra.Command{
	Use:   "get",
	Short: "Get object",
	Long:  `Get specific object from a Peapod.`,
	Args:  cobra.NoArgs,
	Run:   getFunc,
}

func init() {
	common.AddAddressFlag(getCMD, &vAddress)
	common.AddComponentPathFlag(getCMD, &vPath)
	common.AddOutputFileFlag(getCMD, &vOut)
	common.AddPayloadOnlyFlag(getCMD, &vPayloadOnly)
}

func getFunc(cmd *cobra.Command, _ []string) {
	var getPrm blobstorcommon.GetPrm

	err := getPrm.Address.DecodeString(vAddress)
	common.ExitOnErr(cmd, common.Errf("failed to decode object address: %w", err))

	ppd := openPeapod(cmd)
	defer ppd.Close()

	res, err := ppd.Get(getPrm)
	common.ExitOnErr(cmd, common.Errf("failed to read object from Peapod: %w", err))

	common.PrintObjectHeader(cmd, *res.Object)
	if vPayloadOnly {
		common.WriteObjectToFile(cmd, vOut, res.RawData, true)
		return
	}
	common.WriteObjectToFile(cmd, vOut, res.RawData, false)
}
