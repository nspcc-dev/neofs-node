package object

import (
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var objectDelCmd = &cobra.Command{
	Use:     "delete",
	Aliases: []string{"del"},
	Short:   "Delete object from NeoFS",
	Long:    "Delete object from NeoFS",
	Run:     deleteObject,
}

func initObjectDeleteCmd() {
	commonflags.Init(objectDelCmd)
	initFlagSession(objectDelCmd, "DELETE")

	flags := objectDelCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectDelCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectDelCmd.MarkFlagRequired("oid")
}

func deleteObject(cmd *cobra.Command, _ []string) {
	var cnr cid.ID
	var obj oid.ID

	objAddr := readObjectAddress(cmd, &cnr, &obj)
	pk := key.GetOrGenerate(cmd)

	var prm internalclient.DeleteObjectPrm
	ReadOrOpenSession(cmd, &prm, pk, cnr, &obj)
	Prepare(cmd, &prm)
	prm.SetAddress(objAddr)

	res, err := internalclient.DeleteObject(prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	tomb := res.Tombstone()

	cmd.Println("Object removed successfully.")
	cmd.Printf("  ID: %s\n  CID: %s\n", tomb, cnr)
}
