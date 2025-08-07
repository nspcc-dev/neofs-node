package object

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/cheggaaa/pb"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

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
	flags.Bool(rawFlag, false, rawFlagDesc)
	flags.Bool(noProgressFlag, false, "Do not show progress bar")
	flags.Bool(binaryFlag, false, "Serialize whole object structure into given file(id + signature + header + payload).")
}

func getObject(cmd *cobra.Command, _ []string) error {
	var cnr cid.ID
	var obj oid.ID

	objAddr, err := readObjectAddress(cmd, &cnr, &obj)
	if err != nil {
		return err
	}

	var out io.Writer
	filename := cmd.Flag(fileFlag).Value.String()
	if filename == "" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
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

	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	cli, err := internalclient.GetSDKClientByFlag(ctx, commonflags.RPC)
	if err != nil {
		return err
	}
	defer cli.Close()

	var prm internalclient.GetObjectPrm
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

	var p *pb.ProgressBar
	noProgress, _ := cmd.Flags().GetBool(noProgressFlag)

	var payloadWriter io.Writer
	var payloadBuffer *bytes.Buffer
	binary, _ := cmd.Flags().GetBool(binaryFlag)
	if binary {
		payloadBuffer = new(bytes.Buffer)
		payloadWriter = payloadBuffer
	} else {
		payloadWriter = out
	}

	hdr, rdr, err := internalclient.GetObject(ctx, prm)
	if err == nil {
		if filename != "" && !noProgress {
			p = pb.New64(0)
			p.Output = cmd.OutOrStdout()
			p.SetTotal64(int64(hdr.PayloadSize()))
			p.Start()

			payloadWriter = p.NewProxyWriter(payloadWriter)
		}

		if _, err = io.Copy(payloadWriter, rdr); err != nil {
			err = fmt.Errorf("copy payload: %w", err)
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

	if binary {
		objToStore := hdr
		// TODO(@acid-ant): #1932 Use streams to marshal/unmarshal payload
		objToStore.SetPayload(payloadBuffer.Bytes())
		objBytes := objToStore.Marshal()
		_, err = out.Write(objBytes)
		if err != nil {
			return fmt.Errorf("unable to write binary object in out: %w", err)
		}
	}

	if filename != "" && !strictOutput(cmd) {
		cmd.Printf("[%s] Object successfully saved\n", filename)
	}

	// Print header only if file is not streamed to stdout.
	if filename != "" {
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
