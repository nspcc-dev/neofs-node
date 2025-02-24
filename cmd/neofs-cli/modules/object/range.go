package object

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var objectRangeCmd = &cobra.Command{
	Use:   "range",
	Short: "Get payload range data of an object",
	Long:  "Get payload range data of an object",
	Args:  cobra.NoArgs,
	RunE:  getObjectRange,
}

func initObjectRangeCmd() {
	commonflags.Init(objectRangeCmd)
	initFlagSession(objectRangeCmd, "RANGE")

	flags := objectRangeCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = objectRangeCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.String(commonflags.OIDFlag, "", commonflags.OIDFlagUsage)
	_ = objectRangeCmd.MarkFlagRequired(commonflags.OIDFlag)

	flags.String("range", "", "Range to take data from in the form offset:length")
	flags.String(fileFlag, "", "File to write object payload to. Default: stdout.")
	flags.Bool(rawFlag, false, rawFlagDesc)
}

func getObjectRange(cmd *cobra.Command, _ []string) error {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	var obj oid.ID

	objAddr, err := readObjectAddress(cmd, &cnr, &obj)
	if err != nil {
		return err
	}

	ranges, err := getRangeList(cmd)
	if err != nil {
		return err
	}

	if len(ranges) != 1 {
		return fmt.Errorf("exactly one range must be specified, got: %d", len(ranges))
	}

	var out io.Writer

	filename := cmd.Flag(fileFlag).Value.String()
	if filename == "" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			return fmt.Errorf("can't open file '%s': %w", filename, err)
		}

		defer f.Close()

		out = f
	}

	pk, err := key.GetOrGenerate(cmd)
	if err != nil {
		return err
	}

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}

	var prm internalclient.PayloadRangePrm
	prm.SetClient(cli)
	prm.SetPrivateKey(*pk)
	err = Prepare(cmd, &prm)
	if err != nil {
		return err
	}
	err = readSession(cmd, &prm, pk, cnr, obj)
	if err != nil {
		return err
	}

	raw, _ := cmd.Flags().GetBool(rawFlag)
	prm.SetRawFlag(raw)
	prm.SetAddress(objAddr)
	prm.SetRange(ranges[0])
	prm.SetPayloadWriter(out)

	_, err = internalclient.PayloadRange(ctx, prm)
	if err != nil {
		if ok, err := printSplitInfoErr(cmd, err); ok {
			return err
		}

		if err != nil {
			return fmt.Errorf("can't get object payload range: %w", err)
		}
	}

	if filename != "" {
		cmd.Printf("[%s] Payload successfully saved\n", filename)
	}

	return nil
}

func printSplitInfoErr(cmd *cobra.Command, err error) (bool, error) {
	var errSplitInfo *object.SplitInfoError

	ok := errors.As(err, &errSplitInfo)

	if ok {
		cmd.PrintErrln("Object is complex, split information received.")
		if err := printSplitInfo(cmd, errSplitInfo.SplitInfo()); err != nil {
			return false, err
		}
	}

	return ok, nil
}

func printSplitInfo(cmd *cobra.Command, info *object.SplitInfo) error {
	bs, err := marshalSplitInfo(cmd, info)
	if err != nil {
		return fmt.Errorf("can't marshal split info: %w", err)
	}

	cmd.Println(string(bs))
	return nil
}

func marshalSplitInfo(cmd *cobra.Command, info *object.SplitInfo) ([]byte, error) {
	toJSON, _ := cmd.Flags().GetBool(commonflags.JSON)
	toProto, _ := cmd.Flags().GetBool("proto")
	switch {
	case toJSON && toProto:
		return nil, errors.New("'--json' and '--proto' flags are mutually exclusive")
	case toJSON:
		return info.MarshalJSON()
	case toProto:
		return info.Marshal(), nil
	default:
		b := bytes.NewBuffer(nil)
		if splitID := info.SplitID(); splitID != nil {
			b.WriteString("Split ID: " + splitID.String() + "\n")
		}
		if link := info.GetLink(); !link.IsZero() {
			b.WriteString("Linking object: " + link.String() + "\n")
		}
		if first := info.GetFirstPart(); !first.IsZero() {
			b.WriteString("First object: " + first.String() + "\n")
		}
		if last := info.GetLastPart(); !last.IsZero() {
			b.WriteString("Last object: " + last.String() + "\n")
		}
		return b.Bytes(), nil
	}
}

func getRangeList(cmd *cobra.Command) ([]*object.Range, error) {
	v := cmd.Flag("range").Value.String()
	if len(v) == 0 {
		return nil, nil
	}
	vs := strings.Split(v, ",")
	rs := make([]*object.Range, len(vs))
	for i := range vs {
		r := strings.Split(vs[i], rangeSep)
		if len(r) != 2 {
			return nil, fmt.Errorf("invalid range specifier: %s", vs[i])
		}

		offset, err := strconv.ParseUint(r[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid '%s' range offset specifier: %w", vs[i], err)
		}
		length, err := strconv.ParseUint(r[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid '%s' range length specifier: %w", vs[i], err)
		}

		if length == 0 {
			if offset != 0 {
				return nil, fmt.Errorf("invalid '%s' range: zero length with non-zero offset", vs[i])
			}
		} else if offset+length <= offset {
			return nil, fmt.Errorf("invalid '%s' range: uint64 overflow", vs[i])
		}

		rs[i] = object.NewRange()
		rs[i].SetOffset(offset)
		rs[i].SetLength(length)
	}
	return rs, nil
}
