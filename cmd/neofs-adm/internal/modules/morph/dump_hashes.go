package morph

import (
	"bytes"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
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

	zone, _ := cmd.Flags().GetString(customZoneFlag)
	if zone != "" {
		return dumpCustomZoneHashes(cmd, cs.Hash, zone, c)
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

	fillContractVersion(cmd, c, infos)
	printContractInfo(cmd, infos)

	return nil
}

func dumpCustomZoneHashes(cmd *cobra.Command, nnsHash util.Uint160, zone string, c Client) error {
	const nnsMaxTokens = 100

	// The actual signer is not important here.
	account, err := wallet.NewAccount()
	if err != nil {
		return fmt.Errorf("can't create a temporary account: %w", err)
	}

	signers := []actor.SignerAccount{{
		Signer: transaction.Signer{
			Account: account.PrivateKey().GetScriptHash(),
		},
		Account: account,
	}}
	a, err := actor.New(c.(*rpcclient.Client), signers)
	if err != nil {
		return fmt.Errorf("can't get a list of NNS domains: %w", err)
	}

	arr, err := unwrap.Array(a.CallAndExpandIterator(nnsHash, "tokens", nnsMaxTokens))
	if err != nil {
		return fmt.Errorf("can't get a list of NNS domains: %w", err)
	}

	if !strings.HasPrefix(zone, ".") {
		zone = "." + zone
	}

	var infos []contractDumpInfo
	for i := range arr {
		bs, err := arr[i].TryBytes()
		if err != nil {
			continue
		}

		if !bytes.HasSuffix(bs, []byte(zone)) {
			continue
		}

		h, err := nnsResolveHash(c, nnsHash, string(bs))
		if err != nil {
			continue
		}

		infos = append(infos, contractDumpInfo{
			hash: h,
			name: strings.TrimSuffix(string(bs), zone),
		})
	}

	fillContractVersion(cmd, c, infos)
	printContractInfo(cmd, infos)

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

func printContractInfo(cmd *cobra.Command, infos []contractDumpInfo) {
	if len(infos) == 0 {
		return
	}

	buf := bytes.NewBuffer(nil)
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
}

func fillContractVersion(cmd *cobra.Command, c Client, infos []contractDumpInfo) {
	bw := io.NewBufBinWriter()
	sub := io.NewBufBinWriter()
	for i := range infos {
		if infos[i].hash.Equals(util.Uint160{}) {
			emit.Int(bw.BinWriter, 0)
		} else {
			sub.Reset()
			emit.AppCall(sub.BinWriter, infos[i].hash, "version", callflag.NoneFlag)
			if sub.Err != nil {
				panic(fmt.Errorf("BUG: can't create version script: %w", bw.Err))
			}

			script := sub.Bytes()
			emit.Instruction(bw.BinWriter, opcode.TRY, []byte{byte(3 + len(script) + 2), 0})
			bw.BinWriter.WriteBytes(script)
			emit.Instruction(bw.BinWriter, opcode.ENDTRY, []byte{2 + 1})
			emit.Opcodes(bw.BinWriter, opcode.PUSH0)
		}
	}
	emit.Opcodes(bw.BinWriter, opcode.NOP) // for the last ENDTRY target
	if bw.Err != nil {
		panic(fmt.Errorf("BUG: can't create version script: %w", bw.Err))
	}

	res, err := c.InvokeScript(bw.Bytes(), nil)
	if err != nil {
		cmd.Printf("Can't fetch version from NNS: %v\n", err)
		return
	}

	if res.State == vmstate.Halt.String() {
		for i := range res.Stack {
			infos[i].version = parseContractVersion(res.Stack[i])
		}
	}
}
