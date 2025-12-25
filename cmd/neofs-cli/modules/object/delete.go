package object

import (
	"errors"
	"fmt"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
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
	flags.StringSlice(commonflags.SessionSubjectFlag, nil, commonflags.SessionSubjectFlagUsage)
	flags.StringSlice(commonflags.SessionSubjectNNSFlag, nil, commonflags.SessionSubjectNNSFlagUsage)

	_ = objectDelCmd.MarkFlagRequired(commonflags.CIDFlag)
	_ = objectDelCmd.MarkFlagRequired(commonflags.OIDFlag)
}

func deleteObject(cmd *cobra.Command, _ []string) error {
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
				return fmt.Errorf("decode object ID %q string: %w", oIDraw, err)
			}

			objAddr.SetObject(obj)
			objAddrs = append(objAddrs, objAddr)
		}
	}

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	var prm client.PrmObjectDelete
	err = Prepare(cmd, &prm)
	if err != nil {
		return err
	}

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}
	defer cli.Close()

	var statusErr error
	for _, addr := range objAddrs {
		id := addr.Object()
		subjects, err := parseSessionSubjects(cmd, ctx, cli)
		if err != nil {
			return err
		}

		err = ReadOrOpenSessionViaClient(ctx, cmd, &prm, cli, pk, subjects, cnr, id)
		if err != nil {
			return err
		}

		tomb, err := cli.ObjectDelete(ctx, addr.Container(), id, user.NewAutoIDSigner(*pk), prm)
		if err != nil && !errors.Is(err, apistatus.ErrIncomplete) {
			return fmt.Errorf("rpc error: deleting %s object: remove object via client: %w", id, err)
		}

		var statusString = "successfully"
		if errors.Is(err, apistatus.ErrIncomplete) {
			statusString = "partially (incomplete status)"
			statusErr = err
		}

		cmd.Printf("Object %s removed %s.\n", id, statusString)
		cmd.Printf("  ID: %s\n  CID: %s\n", tomb, cnr)
	}
	return statusErr
}
