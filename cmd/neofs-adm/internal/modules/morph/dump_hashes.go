package morph

import (
	"bytes"
	"fmt"
	"text/tabwriter"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-contract/nns"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const lastGlagoliticLetter = 41

type contractDumpInfo struct {
	hash    util.Uint160
	name    string
	version string
}

func dumpContractHashes(cmd *cobra.Command, _ []string) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	cs, err := c.GetContractStateByID(1)
	if err != nil {
		return err
	}

	infos := []contractDumpInfo{{name: nnsContract, hash: cs.Hash}}

	irSize := 0
	for ; irSize < lastGlagoliticLetter; irSize++ {
		ok, err := nnsIsAvailable(c, cs.Hash, getAlphabetNNSDomain(irSize))
		if err != nil {
			return err
		} else if ok {
			break
		}
	}

	buf := bytes.NewBuffer(nil)
	bw := io.NewBufBinWriter()

	if irSize != 0 {
		bw.Reset()
		for i := 0; i < irSize; i++ {
			emit.AppCall(bw.BinWriter, cs.Hash, "resolve", callflag.ReadOnly,
				getAlphabetNNSDomain(i),
				int64(nns.TXT))
		}

		alphaRes, err := c.InvokeScript(bw.Bytes(), nil)
		if err != nil {
			return fmt.Errorf("can't fetch info from NNS: %w", err)
		}

		for i := 0; i < irSize; i++ {
			info := contractDumpInfo{name: fmt.Sprintf("alphabet %d", i)}
			if h, err := parseNNSResolveResult(alphaRes.Stack[i]); err == nil {
				info.hash = h
			}
			infos = append(infos, info)
		}
	}

	for _, ctrName := range contractList {
		bw.Reset()
		emit.AppCall(bw.BinWriter, cs.Hash, "resolve", callflag.ReadOnly,
			ctrName+".neofs", int64(nns.TXT))

		res, err := c.InvokeScript(bw.Bytes(), nil)
		if err != nil {
			return fmt.Errorf("can't fetch info from NNS: %w", err)
		}

		info := contractDumpInfo{name: ctrName}
		if len(res.Stack) != 0 {
			if h, err := parseNNSResolveResult(res.Stack[0]); err == nil {
				info.hash = h
			}
		}
		infos = append(infos, info)
	}

	bw.Reset()
	for i := range infos {
		if infos[i].hash.Equals(util.Uint160{}) {
			emit.Int(bw.BinWriter, 0)
		} else {
			emit.AppCall(bw.BinWriter, infos[i].hash, "version", callflag.NoneFlag)
		}
	}

	res, err := c.InvokeScript(bw.Bytes(), nil)
	if err != nil {
		return fmt.Errorf("can't fetch info from NNS: %w", err)
	}

	if res.State == vmstate.Halt.String() {
		for i := range res.Stack {
			infos[i].version = parseContractVersion(res.Stack[i])
		}
	}

	tw := tabwriter.NewWriter(buf, 0, 2, 2, ' ', 0)
	for _, info := range infos {
		if info.version == "" {
			info.version = "unknown"
		}
		_, _ = tw.Write([]byte(fmt.Sprintf("%s\t(%s):\t%s\n",
			info.name, info.version, info.hash.StringLE())))
	}
	_ = tw.Flush()

	cmd.Print(buf.String())

	return nil
}

func parseContractVersion(item stackitem.Item) string {
	bi, err := item.TryInteger()
	if err != nil || bi.Sign() == 0 || !bi.IsInt64() {
		return "unknown"
	}

	v := bi.Int64()
	major := v / 1_000_000
	minor := (v % 1_000_000) / 1000
	patch := v % 1_000
	return fmt.Sprintf("v%d.%d.%d", major, minor, patch)
}
