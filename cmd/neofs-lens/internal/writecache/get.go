package writecache

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
)

var getCMD = &cobra.Command{
	Use:   "get",
	Short: "Object inspection",
	Long:  `Get specific object from a write-cache.`,
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
	db, err := openWC()
	if err != nil {
		return err
	}
	defer db.Close()

	data, err := writecache.Get(db, []byte(vAddress))
	if err != nil {
		return fmt.Errorf("could not fetch object: %w", err)
	}

	var o object.Object
	if err := o.Unmarshal(data); err != nil {
		return fmt.Errorf("could not unmarshal object: %w", err)
	}

	common.PrintObjectHeader(cmd, o)

	return common.WriteObjectToFile(cmd, vOut, data, vPayloadOnly)
}
