package morph

import (
	"bytes"
	"errors"
	"fmt"
	"text/tabwriter"

	nns "github.com/nspcc-dev/neo-go/examples/nft-nd-nns"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func dumpContractHashes(cmd *cobra.Command, _ []string) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	cs, err := c.GetContractStateByID(1)
	if err != nil {
		return err
	}

	bw := io.NewBufBinWriter()
	for _, ctrName := range contractList {
		emit.AppCall(bw.BinWriter, cs.Hash, "resolve", callflag.ReadOnly,
			ctrName+".neofs", int64(nns.TXT))
	}

	res, err := c.InvokeScript(bw.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("can't fetch info from NNS: %w", err)
	}

	if len(res.Stack) != len(contractList) {
		return errors.New("invalid response from NNS contract: length mismatch")
	}

	buf := bytes.NewBuffer(nil)
	tw := tabwriter.NewWriter(buf, 0, 2, 2, ' ', 0)
	for i := range contractList {
		ctrHash := "hash is invalid"
		bs, err := res.Stack[i].TryBytes()
		if err == nil {
			ctrHash = string(bs) // hashes are stored as hex-encoded LE string
		}

		_, _ = tw.Write([]byte(fmt.Sprintf("%s:\t%s\n", contractList[i], ctrHash)))
	}

	_ = tw.Flush()
	cmd.Print(buf.String())

	return nil
}
