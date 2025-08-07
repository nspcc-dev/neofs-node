package object

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

var objectHeadCmd = &cobra.Command{
	Use:   "head",
	Short: "Get object header",
	Long:  "Get object header",
	Args:  cobra.NoArgs,
	RunE:  getObjectHeader,
}

func initObjectHeadCmd() {
	commonflags.Init(objectHeadCmd)
	initFlagSession(objectHeadCmd, "HEAD")

	flags := objectHeadCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = objectHeadCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.String(commonflags.OIDFlag, "", commonflags.OIDFlagUsage)
	_ = objectHeadCmd.MarkFlagRequired(commonflags.OIDFlag)

	flags.String(fileFlag, "", "File to write header to. Default: stdout.")
	flags.Bool(commonflags.JSON, false, "Marshal output in JSON")
	flags.Bool("proto", false, "Marshal output in Protobuf")
	flags.Bool(rawFlag, false, rawFlagDesc)
}

func getObjectHeader(cmd *cobra.Command, _ []string) error {
	var cnr cid.ID
	var obj oid.ID

	_, err := readObjectAddress(cmd, &cnr, &obj)
	if err != nil {
		return err
	}

	pk, err := key.GetOrGenerate(cmd)
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

	var prm client.PrmObjectHead
	err = Prepare(cmd, &prm)
	if err != nil {
		return err
	}
	err = readSession(cmd, &prm, pk, cnr, obj)
	if err != nil {
		return err
	}

	raw, _ := cmd.Flags().GetBool(rawFlag)
	if raw {
		prm.MarkRaw()
	}

	hdr, err := cli.ObjectHead(ctx, cnr, obj, user.NewAutoIDSigner(*pk), prm)
	if err != nil {
		if ok, err := printSplitInfoErr(cmd, err); ok {
			return err
		}
		if err != nil {
			return fmt.Errorf("rpc error: read object header via client: %w", err)
		}
	}

	err = saveAndPrintHeader(cmd, hdr, cmd.Flag(fileFlag).Value.String())
	if err != nil {
		return err
	}

	return nil
}

func saveAndPrintHeader(cmd *cobra.Command, obj *object.Object, filename string) error {
	bs, err := marshalHeader(cmd, obj)
	if err != nil {
		return fmt.Errorf("could not marshal header: %w", err)
	}
	if len(bs) != 0 {
		if filename == "" {
			cmd.Println(string(bs))
			return nil
		}
		err = os.WriteFile(filename, bs, os.ModePerm)
		if err != nil {
			return fmt.Errorf("could not write header to file: %w", err)
		}
		cmd.Printf("[%s] Header successfully saved.", filename)
	}

	return printHeader(cmd, obj)
}

func marshalHeader(cmd *cobra.Command, hdr *object.Object) ([]byte, error) {
	toJSON, _ := cmd.Flags().GetBool(commonflags.JSON)
	toProto, _ := cmd.Flags().GetBool("proto")
	switch {
	case toJSON && toProto:
		return nil, errors.New("'--json' and '--proto' flags are mutually exclusive")
	case toJSON:
		return hdr.MarshalJSON()
	case toProto:
		return hdr.Marshal(), nil
	default:
		return nil, nil
	}
}

func printObjectID(cmd *cobra.Command, recv func() oid.ID) {
	var strID string

	id := recv()
	if !id.IsZero() {
		strID = id.String()
	} else {
		strID = "<empty>"
	}

	cmd.Printf("ID: %s\n", strID)
}

func printContainerID(cmd *cobra.Command, recv func() cid.ID) {
	var strID string

	id := recv()
	if !id.IsZero() {
		strID = id.String()
	} else {
		strID = "<empty>"
	}

	cmd.Printf("CID: %s\n", strID)
}

func printHeader(cmd *cobra.Command, obj *object.Object) error {
	printObjectID(cmd, obj.GetID)
	printContainerID(cmd, obj.GetContainerID)
	cmd.Printf("Owner: %s\n", obj.Owner())
	cmd.Printf("CreatedAt: %d\n", obj.CreationEpoch())
	cmd.Printf("Size: %d\n", obj.PayloadSize())
	common.PrintChecksum(cmd, "HomoHash", obj.PayloadHomomorphicHash)
	common.PrintChecksum(cmd, "Checksum", obj.PayloadChecksum)
	cmd.Printf("Type: %s\n", obj.Type())

	cmd.Println("Attributes:")
	for _, attr := range obj.Attributes() {
		if attr.Key() == object.AttributeTimestamp {
			cmd.Printf("  %s=%s (%s)\n",
				attr.Key(),
				attr.Value(),
				common.PrettyPrintUnixTime(attr.Value()))
			continue
		}
		cmd.Printf("  %s=%s\n", attr.Key(), attr.Value())
	}

	if signature := obj.Signature(); signature != nil {
		cmd.Print("ID signature:\n")
		cmd.Printf("  public key: %s\n", hex.EncodeToString(signature.PublicKeyBytes()))
		cmd.Printf("  signature: %s\n", hex.EncodeToString(signature.Value()))
	}

	return printSplitHeader(cmd, obj)
}

func printSplitHeader(cmd *cobra.Command, obj *object.Object) error {
	if splitID := obj.SplitID(); splitID != nil {
		cmd.Printf("Split ID: %s\n", splitID)
	}

	if oID := obj.GetParentID(); !oID.IsZero() {
		cmd.Printf("Split ParentID: %s\n", oID)
	}

	if prev := obj.GetPreviousID(); !prev.IsZero() {
		cmd.Printf("Split PreviousID: %s\n", prev)
	}

	if first, ok := obj.FirstID(); ok {
		cmd.Printf("Split FirstID: %s\n", first)
	}

	for _, child := range obj.Children() {
		cmd.Printf("Split ChildID: %s\n", child.String())
	}

	parent := obj.Parent()
	if parent != nil {
		cmd.Print("\nSplit Parent Header:\n")

		return printHeader(cmd, parent)
	}

	return nil
}
