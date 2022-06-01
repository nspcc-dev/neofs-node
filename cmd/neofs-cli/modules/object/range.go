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
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	sessionCli "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/modules/session"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var objectRangeCmd = &cobra.Command{
	Use:   "range",
	Short: "Get payload range data of an object",
	Long:  "Get payload range data of an object",
	Run:   getObjectRange,
}

func initObjectRangeCmd() {
	commonflags.Init(objectRangeCmd)
	commonflags.InitSession(objectRangeCmd)

	flags := objectRangeCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectRangeCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectRangeCmd.MarkFlagRequired("oid")

	flags.String("range", "", "Range to take data from in the form offset:length")
	flags.String("file", "", "File to write object payload to. Default: stdout.")
	flags.Bool(rawFlag, false, rawFlagDesc)
}

func getObjectRange(cmd *cobra.Command, _ []string) {
	var cnr cid.ID
	var obj oid.ID

	objAddr := readObjectAddress(cmd, &cnr, &obj)

	ranges, err := getRangeList(cmd)
	common.ExitOnErr(cmd, "", err)

	if len(ranges) != 1 {
		common.ExitOnErr(cmd, "", fmt.Errorf("exactly one range must be specified, got: %d", len(ranges)))
	}

	var out io.Writer

	filename := cmd.Flag("file").Value.String()
	if filename == "" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			common.ExitOnErr(cmd, "", fmt.Errorf("can't open file '%s': %w", filename, err))
		}

		defer f.Close()

		out = f
	}

	pk := key.GetOrGenerate(cmd)

	var prm internalclient.PayloadRangePrm
	sessionCli.Prepare(cmd, cnr, &obj, pk, &prm)
	Prepare(cmd, &prm)

	raw, _ := cmd.Flags().GetBool(rawFlag)
	prm.SetRawFlag(raw)
	prm.SetAddress(objAddr)
	prm.SetRange(ranges[0])
	prm.SetPayloadWriter(out)

	_, err = internalclient.PayloadRange(prm)
	if err != nil {
		if ok := printSplitInfoErr(cmd, err); ok {
			return
		}

		common.ExitOnErr(cmd, "can't get object payload range: %w", err)
	}

	if filename != "" {
		cmd.Printf("[%s] Payload successfully saved\n", filename)
	}
}

func printSplitInfoErr(cmd *cobra.Command, err error) bool {
	var errSplitInfo *object.SplitInfoError

	ok := errors.As(err, &errSplitInfo)

	if ok {
		cmd.PrintErrln("Object is complex, split information received.")
		printSplitInfo(cmd, errSplitInfo.SplitInfo())
	}

	return ok
}

func printSplitInfo(cmd *cobra.Command, info *object.SplitInfo) {
	bs, err := marshalSplitInfo(cmd, info)
	common.ExitOnErr(cmd, "can't marshal split info: %w", err)

	cmd.Println(string(bs))
}

func marshalSplitInfo(cmd *cobra.Command, info *object.SplitInfo) ([]byte, error) {
	toJSON, _ := cmd.Flags().GetBool("json")
	toProto, _ := cmd.Flags().GetBool("proto")
	switch {
	case toJSON && toProto:
		return nil, errors.New("'--json' and '--proto' flags are mutually exclusive")
	case toJSON:
		return info.MarshalJSON()
	case toProto:
		return info.Marshal()
	default:
		b := bytes.NewBuffer(nil)
		if splitID := info.SplitID(); splitID != nil {
			b.WriteString("Split ID: " + splitID.String() + "\n")
		}
		if link, ok := info.Link(); ok {
			b.WriteString("Linking object: " + link.String() + "\n")
		}
		if last, ok := info.LastPart(); ok {
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
			return nil, fmt.Errorf("invalid range specifier: %s", vs[i])
		}
		length, err := strconv.ParseUint(r[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid range specifier: %s", vs[i])
		}
		rs[i] = object.NewRange()
		rs[i].SetOffset(offset)
		rs[i].SetLength(length)
	}
	return rs, nil
}
