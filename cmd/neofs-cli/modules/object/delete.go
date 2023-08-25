package object

import (
	"fmt"

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
	Args:    cobra.NoArgs,
	Run:     deleteObject,
}

func initObjectDeleteCmd() {
	commonflags.Init(objectDelCmd)
	initFlagSession(objectDelCmd, "DELETE")

	flags := objectDelCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	flags.String(commonflags.OIDFlag, "", commonflags.OIDFlagUsage)
	flags.Bool(binaryFlag, false, "Deserialize object structure from given file.")
	flags.String(fileFlag, "", "File with object payload")
}

func deleteObject(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	var obj oid.ID
	var objAddr oid.Address

	binary, _ := cmd.Flags().GetBool(binaryFlag)
	if binary {
		filename, _ := cmd.Flags().GetString(fileFlag)
		if filename == "" {
			common.ExitOnErr(cmd, "", fmt.Errorf("required flag \"%s\" not set", fileFlag))
		}
		objAddr = readObjectAddressBin(cmd, &cnr, &obj, filename)
	} else {
		cidVal, _ := cmd.Flags().GetString(commonflags.CIDFlag)
		if cidVal == "" {
			common.ExitOnErr(cmd, "", fmt.Errorf("required flag \"%s\" not set", commonflags.CIDFlag))
		}

		oidVal, _ := cmd.Flags().GetString(commonflags.OIDFlag)
		if oidVal == "" {
			common.ExitOnErr(cmd, "", fmt.Errorf("required flag \"%s\" not set", commonflags.OIDFlag))
		}

		objAddr = readObjectAddress(cmd, &cnr, &obj)
	}

	pk := key.GetOrGenerate(cmd)

	var prm internalclient.DeleteObjectPrm
	ReadOrOpenSession(ctx, cmd, &prm, pk, cnr, &obj)
	Prepare(cmd, &prm)
	prm.SetPrivateKey(*pk)
	prm.SetAddress(objAddr)

	res, err := internalclient.DeleteObject(ctx, prm)
	common.ExitOnErr(cmd, "rpc error: %w", err)

	tomb := res.Tombstone()

	cmd.Println("Object removed successfully.")
	cmd.Printf("  ID: %s\n  CID: %s\n", tomb, cnr)
}
