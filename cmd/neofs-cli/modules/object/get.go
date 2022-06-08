package object

import (
	"fmt"
	"io"
	"os"

	"github.com/cheggaaa/pb"
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

var objectGetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get object from NeoFS",
	Long:  "Get object from NeoFS",
	Run:   getObject,
}

func initObjectGetCmd() {
	commonflags.Init(objectGetCmd)
	commonflags.InitSession(objectGetCmd)

	flags := objectGetCmd.Flags()

	flags.String("cid", "", "Container ID")
	_ = objectGetCmd.MarkFlagRequired("cid")

	flags.String("oid", "", "Object ID")
	_ = objectGetCmd.MarkFlagRequired("oid")

	flags.String("file", "", "File to write object payload to. Default: stdout.")
	flags.String("header", "", "File to write header to. Default: stdout.")
	flags.Bool(rawFlag, false, rawFlagDesc)
	flags.Bool(noProgressFlag, false, "Do not show progress bar")
}

func getObject(cmd *cobra.Command, _ []string) {
	var cnr cid.ID
	var obj oid.ID

	objAddr := readObjectAddress(cmd, &cnr, &obj)

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

	var prm internalclient.GetObjectPrm
	sessionCli.Prepare(cmd, cnr, &obj, pk, &prm)
	Prepare(cmd, &prm)

	raw, _ := cmd.Flags().GetBool(rawFlag)
	prm.SetRawFlag(raw)
	prm.SetAddress(objAddr)

	var p *pb.ProgressBar
	noProgress, _ := cmd.Flags().GetBool(noProgressFlag)

	if filename == "" || noProgress {
		prm.SetPayloadWriter(out)
	} else {
		p = pb.New64(0)
		p.Output = cmd.OutOrStdout()
		prm.SetPayloadWriter(p.NewProxyWriter(out))
		prm.SetHeaderCallback(func(o *object.Object) {
			p.SetTotal64(int64(o.PayloadSize()))
			p.Start()
		})
	}

	res, err := internalclient.GetObject(prm)
	if p != nil {
		p.Finish()
	}
	if err != nil {
		if ok := printSplitInfoErr(cmd, err); ok {
			return
		}

		common.ExitOnErr(cmd, "rpc error: %w", err)
	}

	hdrFile := cmd.Flag("header").Value.String()
	if filename != "" {
		if hdrFile != "" || !strictOutput(cmd) {
			cmd.Printf("[%s] Object successfully saved\n", filename)
		}
	}

	// Print header only if file is not streamed to stdout.
	if filename != "" || hdrFile != "" {
		err = saveAndPrintHeader(cmd, res.Header(), hdrFile)
		common.ExitOnErr(cmd, "", err)
	}
}

func strictOutput(cmd *cobra.Command) bool {
	toJSON, _ := cmd.Flags().GetBool("json")
	toProto, _ := cmd.Flags().GetBool("proto")
	return toJSON || toProto
}
