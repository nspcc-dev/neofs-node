package object

import (
	"fmt"
	"io"
	"os"

	"github.com/cheggaaa/pb"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	"github.com/nspcc-dev/neofs-node/internal/object"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	sdkobject "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/spf13/cobra"
)

const payloadOnlyFlag = "payload-only"

var objectGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get object from NeoFS",
	Long:  "Get object from NeoFS",
	Args:  cobra.NoArgs,
	RunE:  getObject,
}

func initObjectGetCmd() {
	commonflags.Init(objectGetCmd)
	initFlagSession(objectGetCmd, "GET")

	flags := objectGetCmd.Flags()

	flags.String(commonflags.CIDFlag, "", commonflags.CIDFlagUsage)
	_ = objectGetCmd.MarkFlagRequired(commonflags.CIDFlag)

	flags.String(commonflags.OIDFlag, "", commonflags.OIDFlagUsage)
	_ = objectGetCmd.MarkFlagRequired(commonflags.OIDFlag)

	flags.String(fileFlag, "", "File to write object payload to(with -b together with signature and header). Default: stdout.")
	flags.String(rangeFlag, "", rangeFlagUsage)
	flags.Bool(rawFlag, false, rawFlagDesc)
	flags.Bool(noProgressFlag, false, "Do not show progress bar")
	flags.Bool(binaryFlag, false, "Serialize whole object structure into given file(id + signature + header + payload).")
	flags.Bool(payloadOnlyFlag, false, "Request only payload for a payload range")
}

func getObject(cmd *cobra.Command, _ []string) error {
	var cnr cid.ID
	var obj oid.ID

	_, err := readObjectAddress(cmd, &cnr, &obj)
	if err != nil {
		return err
	}

	var out io.Writer
	filename := cmd.Flag(fileFlag).Value.String()
	if filename == "" {
		out = os.Stdout
	} else {
		f, err := openFileForPayload(filename)
		if err != nil {
			return err
		}

		defer func() { _ = f.Close() }()

		out = f
	}

	ranges, err := getRangeList(cmd)
	if err != nil {
		return err
	}
	if len(ranges) > 1 {
		return fmt.Errorf("at most one range can be specified, got: %d", len(ranges))
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
	defer func() { _ = cli.Close() }()

	var prm client.PrmObjectGet
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

	var p *pb.ProgressBar
	noProgress, _ := cmd.Flags().GetBool(noProgressFlag)

	binary, _ := cmd.Flags().GetBool(binaryFlag)
	payloadOnly, _ := cmd.Flags().GetBool(payloadOnlyFlag)
	if len(ranges) != 0 {
		if binary {
			return fmt.Errorf("--%s cannot be used with --%s", binaryFlag, rangeFlag)
		}
		prm.SetRange(ranges[0].GetOffset(), ranges[0].GetLength())
	}
	if payloadOnly {
		prm.MarkPayloadOnly()
	}

	hdr, rdr, err := cli.ObjectGetInit(ctx, cnr, obj, user.NewAutoIDSigner(*pk), prm)
	if err == nil {
		// In binary mode, write header (without payload) and protobuf payload field tag+length, then stream payload
		if binary {
			if wErr := object.WriteWithoutPayload(out, hdr); wErr != nil {
				err = fmt.Errorf("write object header: %w", wErr)
			}
		}

		if filename != "" && !noProgress && !payloadOnly {
			p = pb.New64(payloadReadSize(hdr.PayloadSize(), ranges))
			p.Output = cmd.OutOrStdout()
			p.Start()

			out = p.NewProxyWriter(out)
		}

		if err == nil {
			if _, err = io.Copy(out, rdr); err != nil {
				err = fmt.Errorf("copy payload: %w", err)
			}
		}
	}
	if p != nil {
		p.Finish()
	}
	if err != nil {
		if ok, err := printSplitInfoErr(cmd, err); ok {
			return err
		}
		if err != nil {
			return fmt.Errorf("rpc error: %w", err)
		}
	}

	if filename != "" && !strictOutput(cmd) {
		cmd.Printf("[%s] Object successfully saved\n", filename)
	}

	// Print header only if file is not streamed to stdout.
	if filename != "" && !payloadOnly {
		err = printHeader(cmd, &hdr)
		if err != nil {
			return err
		}
	}
	return nil
}

func strictOutput(cmd *cobra.Command) bool {
	toJSON, _ := cmd.Flags().GetBool(commonflags.JSON)
	toProto, _ := cmd.Flags().GetBool("proto")
	return toJSON || toProto
}

func payloadReadSize(payloadSize uint64, ranges []*sdkobject.Range) int64 {
	if len(ranges) == 0 {
		return int64(payloadSize)
	}

	rng := ranges[0]
	if ln := rng.GetLength(); ln != 0 {
		return int64(ln)
	}
	if off := rng.GetOffset(); off < payloadSize {
		return int64(payloadSize - off)
	}
	return 0
}
