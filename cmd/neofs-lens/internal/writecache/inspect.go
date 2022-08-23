package writecache

import (
	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/spf13/cobra"
)

var inspectCMD = &cobra.Command{
	Use:   "inspect",
	Short: "Object inspection",
	Long:  `Inspect specific object in a write-cache.`,
	Run:   inspectFunc,
}

func init() {
	common.AddAddressFlag(inspectCMD, &vAddress)
	common.AddComponentPathFlag(inspectCMD, &vPath)
	common.AddOutputFileFlag(inspectCMD, &vOut)
}

func inspectFunc(cmd *cobra.Command, _ []string) {
	db, err := writecache.OpenDB(vPath, true)
	common.ExitOnErr(cmd, common.Errf("could not open write-cache db: %w", err))

	defer db.Close()

	data, err := writecache.Get(db, []byte(vAddress))
	common.ExitOnErr(cmd, common.Errf("could not fetch object: %w", err))

	var o object.Object
	common.ExitOnErr(cmd, common.Errf("could not unmarshal object: %w", o.Unmarshal(data)))

	common.PrintObjectHeader(cmd, o)
	common.WriteObjectToFile(cmd, vOut, data)
}
