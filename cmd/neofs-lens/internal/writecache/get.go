package writecache

import (
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
	Run:   getFunc,
}

func init() {
	common.AddAddressFlag(getCMD, &vAddress)
	common.AddComponentPathFlag(getCMD, &vPath)
	common.AddOutputFileFlag(getCMD, &vOut)
	common.AddPayloadOnlyFlag(getCMD, &vPayloadOnly)
}

func getFunc(cmd *cobra.Command, _ []string) {
	db := openWC(cmd)
	defer db.Close()

	data, err := writecache.Get(db, []byte(vAddress))
	common.ExitOnErr(cmd, common.Errf("could not fetch object: %w", err))

	var o object.Object
	common.ExitOnErr(cmd, common.Errf("could not unmarshal object: %w", o.Unmarshal(data)))

	common.PrintObjectHeader(cmd, o)
	if vPayloadOnly {
		common.WriteObjectToFile(cmd, vOut, data, true)
		return
	}
	common.WriteObjectToFile(cmd, vOut, data, false)
}
