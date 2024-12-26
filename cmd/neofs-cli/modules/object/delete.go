package object

import (
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
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
	RunE:    deleteObject,
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

func deleteObject(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	var objAddrs []oid.Address

	binary, _ := cmd.Flags().GetBool(binaryFlag)
	if binary {
		filename, _ := cmd.Flags().GetString(fileFlag)
		if filename == "" {
			return fmt.Errorf("required flag \"%s\" not set", fileFlag)
		}
		var obj oid.ID
		objAddr, err := readObjectAddressBin(&cnr, &obj, filename)
		if err != nil {
			return err
		}

		objAddrs = []oid.Address{objAddr}
	} else {
		err := readCID(cmd, &cnr)
		if err != nil {
			return err
		}
		oIDVals, _ := cmd.Flags().GetStringSlice(commonflags.OIDFlag)
		if len(oIDVals) == 0 {
			return fmt.Errorf("provide at least one object via %s flag", commonflags.OIDFlag)
		}

		var obj oid.ID
		var objAddr oid.Address
		objAddr.SetContainer(cnr)

		for _, oIDraw := range oIDVals {
			err := obj.DecodeString(oIDraw)
			if err != nil {
				return fmt.Errorf("decode object ID (\"+oIDraw+\") string: %w", err)
			}

			objAddr.SetObject(obj)
			objAddrs = append(objAddrs, objAddr)
		}
	}

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	var prm internalclient.DeleteObjectPrm
	prm.SetPrivateKey(*pk)
	err = Prepare(cmd, &prm)
	if err != nil {
		return err
	}

	for _, addr := range objAddrs {
		id := addr.Object()
		err := ReadOrOpenSession(ctx, cmd, &prm, pk, cnr, id)
		if err != nil {
			return err
		}
		prm.SetAddress(addr)

		res, err := internalclient.DeleteObject(ctx, prm)
		if err != nil {
			return fmt.Errorf("rpc error: deleting %s object: %w", id, err)
		}

		tomb := res.Tombstone()

		cmd.Printf("Object %s removed successfully.\n", id)
		cmd.Printf("  ID: %s\n  CID: %s\n", tomb, cnr)
	}
	return nil
}
