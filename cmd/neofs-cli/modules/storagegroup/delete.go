package storagegroup

import (
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	objectCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/object"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var sgDelCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete storage group from NeoFS",
	Long:  "Delete storage group from NeoFS",
	Args:  cobra.NoArgs,
	Run:   delSG,
}

func initSGDeleteCmd() {
	commonflags.Init(sgDelCmd)

	flags := sgDelCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgDelCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.StringVarP(&sgID, sgIDFlag, "", "", "Storage group identifier")
	_ = sgDelCmd.MarkFlagRequired(sgIDFlag)
}

func delSG(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	pk := key.GetOrGenerate(cmd)

	var cnr cid.ID
	var obj oid.ID

	addr := readObjectAddress(cmd, &cnr, &obj)

	var prm internalclient.DeleteObjectPrm
	prm.SetPrivateKey(*pk)
	objectCli.OpenSession(ctx, cmd, &prm, pk, cnr, &obj)
	objectCli.Prepare(cmd, &prm)
	prm.SetAddress(addr)

	res, err := internalclient.DeleteObject(ctx, prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	tombstone := res.Tombstone()

	cmd.Println("Storage group removed successfully.")
	cmd.Printf("  Tombstone: %s\n", tombstone)
}
