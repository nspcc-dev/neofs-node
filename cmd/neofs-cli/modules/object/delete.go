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
	flags.StringSlice(commonflags.OIDFlag, nil, commonflags.OIDFlagUsage)
	flags.Bool(binaryFlag, false, "Deserialize object structure from given file.")
	flags.String(fileFlag, "", "File with object payload")

	_ = objectDelCmd.MarkFlagRequired(commonflags.CIDFlag)
	_ = objectDelCmd.MarkFlagRequired(commonflags.OIDFlag)
}

func deleteObject(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	var objAddrs []oid.Address

	binary, _ := cmd.Flags().GetBool(binaryFlag)
	if binary {
		filename, _ := cmd.Flags().GetString(fileFlag)
		if filename == "" {
			common.ExitOnErr(cmd, "", fmt.Errorf("required flag \"%s\" not set", fileFlag))
		}
		var obj oid.ID
		objAddr := readObjectAddressBin(cmd, &cnr, &obj, filename)

		objAddrs = []oid.Address{objAddr}
	} else {
		readCID(cmd, &cnr)
		oIDVals, _ := cmd.Flags().GetStringSlice(commonflags.OIDFlag)
		if len(oIDVals) == 0 {
			common.ExitOnErr(cmd, "", fmt.Errorf("provide at least one object via %s flag", commonflags.OIDFlag))
		}

		var obj oid.ID
		var objAddr oid.Address
		objAddr.SetContainer(cnr)

		for _, oIDraw := range oIDVals {
			err := obj.DecodeString(oIDraw)
			common.ExitOnErr(cmd, "decode object ID ("+oIDraw+") string: %w", err)

			objAddr.SetObject(obj)
			objAddrs = append(objAddrs, objAddr)
		}
	}

	pk := key.GetOrGenerate(cmd)

	var prm internalclient.DeleteObjectPrm
	prm.SetPrivateKey(*pk)
	Prepare(cmd, &prm)

	for _, addr := range objAddrs {
		ReadOrOpenSession(ctx, cmd, &prm, pk, cnr, addr.Object())
		prm.SetAddress(addr)

		res, err := internalclient.DeleteObject(ctx, prm)
		common.ExitOnErr(cmd, "rpc error: deleting "+addr.Object().String()+" object: %w", err)

		tomb := res.Tombstone()

		cmd.Printf("Object %s removed successfully.\n", addr.Object())
		cmd.Printf("  ID: %s\n  CID: %s\n", tomb, cnr)
	}
}
