package object

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/cheggaaa/pb"
	internalclient "github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/client"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/common"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/commonflags"
	"github.com/nspcc-dev/neofs-node/cmd/neofs-cli/internal/key"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/spf13/cobra"
)

var objectGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get object from NeoFS",
	Long:  "Get object from NeoFS",
	Args:  cobra.NoArgs,
	Run:   getObject,
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

func getObject(cmd *cobra.Command, _ []string) {
	ctx, cancel := commonflags.GetCommandContext(cmd)
	defer cancel()

	var cnr cid.ID
	var obj oid.ID

	objAddr := readObjectAddress(cmd, &cnr, &obj)

	var out io.Writer
	filename := cmd.Flag(fileFlag).Value.String()
	if filename == "" {
		out = os.Stdout
	} else {
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			common.ExitOnErr(cmd, "", fmt.Errorf("can't open file '%s': %w", filename, err))
		}

		defer f.Close()

		out = f
	}

	pk := key.GetOrGenerate(cmd)

	cli := internalclient.GetSDKClientByFlag(ctx, cmd, commonflags.RPC)

	var prm internalclient.GetObjectPrm
	prm.SetClient(cli)
	prm.SetPrivateKey(*pk)
	Prepare(cmd, &prm)
	readSession(cmd, &prm, pk, cnr, obj)

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

	if filename == "" || noProgress {
		prm.SetPayloadWriter(payloadWriter)
	} else {
		p = pb.New64(0)
		p.Output = cmd.OutOrStdout()
		prm.SetPayloadWriter(p.NewProxyWriter(payloadWriter))
		prm.SetHeaderCallback(func(o *object.Object) {
			p.SetTotal64(int64(o.PayloadSize()))
			p.Start()
		})
	}

	res, err := internalclient.GetObject(ctx, prm)
	if p != nil {
		p.Finish()
	}
	if err != nil {
		if ok := printSplitInfoErr(cmd, err); ok {
			return
		}

		common.ExitOnErr(cmd, "rpc error: %w", err)
	}

	if binary {
		objToStore := res.Header()
		// TODO(@acid-ant): #1932 Use streams to marshal/unmarshal payload
		objToStore.SetPayload(payloadBuffer.Bytes())
		objBytes, err := objToStore.Marshal()
		common.ExitOnErr(cmd, "", err)
		_, err = out.Write(objBytes)
		common.ExitOnErr(cmd, "unable to write binary object in out: %w ", err)
	}

	if filename != "" && !strictOutput(cmd) {
		cmd.Printf("[%s] Object successfully saved\n", filename)
	}

	// Print header only if file is not streamed to stdout.
	if filename != "" {
		err = printHeader(cmd, res.Header())
		common.ExitOnErr(cmd, "", err)
	}
}

func strictOutput(cmd *cobra.Command) bool {
	toJSON, _ := cmd.Flags().GetBool(commonflags.JSON)
	toProto, _ := cmd.Flags().GetBool("proto")
	return toJSON || toProto
}
