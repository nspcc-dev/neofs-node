package storagegroup

import (
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
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
	RunE:  delSG,
}

func initSGDeleteCmd() {
	commonflags.Init(sgDelCmd)

	flags := sgDelCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = sgDelCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.StringVarP(&sgID, sgIDFlag, "", "", "Storage group identifier")
	_ = sgDelCmd.MarkFlagRequired(sgIDFlag)
}

func delSG(cmd *cobra.Command, _ []string) error {
	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	var cnr cid.ID
	var obj oid.ID

	addr, err := readObjectAddress(cmd, &cnr, &obj)
	if err != nil {
		return err
	}

	var prm internalclient.DeleteObjectPrm
	prm.SetPrivateKey(*pk)

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	err = objectCli.OpenSession(ctx, cmd, &prm, pk, cnr, obj)
	if err != nil {
		return err
	}
	err = objectCli.Prepare(cmd, &prm)
	if err != nil {
		return err
	}
	prm.SetAddress(addr)

	res, err := internalclient.DeleteObject(ctx, prm)
	if err != nil {
		return fmt.Errorf("rpc error: %w", err)
	}

	tombstone := res.Tombstone()

	cmd.Println("Storage group removed successfully.")
	cmd.Printf("  Tombstone: %s\n", tombstone)

	return nil
}
