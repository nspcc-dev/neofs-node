package meta

import (
	"errors"
	"fmt"

	common "github.com/nspcc-dev/neofs-node/cmd/neofs-lens/internal"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var getCMD = &cobra.Command{
	Use:   "get",
	Short: "Object inspection",
	Long:  `Get specific object from a metabase.`,
	Args:  cobra.NoArgs,
	Run:   getFunc,
}

func init() {
	common.AddAddressFlag(getCMD, &vAddress)
	common.AddComponentPathFlag(getCMD, &vPath)
}

func getFunc(cmd *cobra.Command, _ []string) {
	var addr oid.Address

	err := addr.DecodeString(vAddress)
	common.ExitOnErr(cmd, common.Errf("invalid address argument: %w", err))

	db := openMeta(cmd, true)
	defer db.Close()

	storageID := meta.StorageIDPrm{}
	storageID.SetAddress(addr)

	resStorageID, err := db.StorageID(storageID)
	common.ExitOnErr(cmd, common.Errf("could not check if the obj is small: %w", err))

	if id := resStorageID.StorageID(); id != nil {
		cmd.Printf("Object storageID: %x (%q)\n\n", id, id)
	} else {
		cmd.Printf("Object does not contain storageID\n\n")
	}

	prm := meta.GetPrm{}
	prm.SetAddress(addr)
	prm.SetRaw(true)

	siErr := new(object.SplitInfoError)

	res, err := db.Get(prm)
	if errors.As(err, &siErr) {
		link, linkSet := siErr.SplitInfo().Link()
		last, lastSet := siErr.SplitInfo().LastPart()

		fmt.Println("Object is split")
		cmd.Println("\tSplitID:", siErr.SplitInfo().SplitID().String())

		if linkSet {
			cmd.Println("\tLink:", link)
		}
		if lastSet {
			cmd.Println("\tLast:", last)
		}

		return
	}
	common.ExitOnErr(cmd, common.Errf("could not get object: %w", err))

	common.PrintObjectHeader(cmd, *res.Header())
}
