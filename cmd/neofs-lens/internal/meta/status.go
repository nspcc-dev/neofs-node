package meta

import (
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var statCMD = &cobra.Command{
	Use:   "status",
	Short: "Object status information",
	Long:  `Get metabase's indexes related to an object.`,
	Args:  cobra.NoArgs,
	Run:   statusFunc,
}

func init() {
	common.AddAddressFlag(statCMD, &vAddress)
	common.AddComponentPathFlag(statCMD, &vPath)
}

func statusFunc(cmd *cobra.Command, _ []string) {
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	common.ExitOnErr(cmd, common.Errf("invalid address argument: %w", err))

	db := openMeta(cmd, true)
	defer db.Close()

	res, err := db.ObjectStatus(addr)
	common.ExitOnErr(cmd, common.Errf("reading object status: %w", err))

	const emptyValPlaceholder = "<empty>"
	storageID := res.StorageID
	if storageID == "" {
		storageID = emptyValPlaceholder
	}

	cmd.Printf("Metabase version: %d\n", res.Version)
	cmd.Printf("Object state: %s\n", res.State)
	cmd.Printf("Storage's ID: %s\n", storageID)
	cmd.Println("Indexes:")
	for _, bucket := range res.Buckets {
		valStr := emptyValPlaceholder
		if bucket.Value != nil {
			valStr = fmt.Sprintf("%x", bucket.Value)
		}

		cmd.Printf("\tBucket: %d\n"+
			"\tValue (HEX): %s\n", bucket.BucketIndex, valStr)
	}
}
