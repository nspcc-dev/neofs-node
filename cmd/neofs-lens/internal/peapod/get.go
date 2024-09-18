package peapod

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	blobstorcommon "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/spf13/cobra"
)

var getCMD = &cobra.Command{
	Use:   "get",
	Short: "Get object",
	Long:  `Get specific object from a Peapod.`,
	Args:  cobra.NoArgs,
	RunE:  getFunc,
}

func init() {
	common.AddAddressFlag(getCMD, &vAddress)
	common.AddComponentPathFlag(getCMD, &vPath)
	common.AddOutputFileFlag(getCMD, &vOut)
	common.AddPayloadOnlyFlag(getCMD, &vPayloadOnly)
}

func getFunc(cmd *cobra.Command, _ []string) error {
	var getPrm blobstorcommon.GetPrm

	err := getPrm.Address.DecodeString(vAddress)
	if err != nil {
		return fmt.Errorf("failed to decode object address: %w", err)
	}

	ppd, err := openPeapod()
	if err != nil {
		return err
	}
	defer ppd.Close()

	res, err := ppd.Get(getPrm)
	if err != nil {
		return fmt.Errorf("failed to read object from Peapod: %w", err)
	}

	common.PrintObjectHeader(cmd, *res.Object)
	if vPayloadOnly {
		if err := common.WriteObjectToFile(cmd, vOut, res.RawData, true); err != nil {
			return err
		}
		return nil
	}
	if err := common.WriteObjectToFile(cmd, vOut, res.RawData, false); err != nil {
		return err
	}

	return nil
}
