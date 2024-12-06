package peapod

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	if err != nil {
		return fmt.Errorf("failed to decode object address: %w", err)
	}

	ppd, err := openPeapod()
	if err != nil {
		return err
	}
	defer ppd.Close()

	data, err := ppd.GetBytes(addr)
	if err != nil {
		return fmt.Errorf("failed to read object from Peapod: %w", err)
	}

	obj := objectSDK.New()
	if err := obj.Unmarshal(data); err != nil {
		return fmt.Errorf("decode object from binary: %w", err)
	}

	common.PrintObjectHeader(cmd, *obj)
	if vPayloadOnly {
		if err := common.WriteObjectToFile(cmd, vOut, data, true); err != nil {
			return err
		}
		return nil
	}
	if err := common.WriteObjectToFile(cmd, vOut, data, false); err != nil {
		return err
	}

	return nil
}
