package fschain

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep11"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const lastGlagoliticLetter = 41

type contractDumpInfo struct {
	hash       util.Uint160
	name       string
	version    string
	expiration int64
}

func dumpContractHashes(cmd *cobra.Command, _ []string) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	inv := invoker.New(c, nil)
	nnsHash, err := nns.InferHash(c)
	if err != nil {
		return fmt.Errorf("can't get NNS contract hash: %w", err)
	}
	nnsReader := nns.NewReader(inv, nnsHash)

	zone, _ := cmd.Flags().GetString(customZoneFlag)
	if zone != "" {
		return dumpCustomZoneHashes(cmd, nnsHash, zone, c)
	}

	infos := []contractDumpInfo{{name: nnsContract, hash: nnsHash}}

	irSize := 0
	for ; irSize < lastGlagoliticLetter; irSize++ {
		ok, err := nnsReader.IsAvailable(getAlphabetNNSDomain(irSize) + "." + nns.ContractTLD)
		if err != nil {
			return err
		} else if ok {
			break
		}
	}

	var contracts = make([]string, 0, irSize+len(contractList))
	for i := range irSize {
		contracts = append(contracts, getAlphabetNNSDomain(i))
	}
	contracts = append(contracts, contractList...)

	for _, ctrName := range contracts {
		h, err := nnsReader.ResolveFSContract(ctrName)
		if err != nil {
			return fmt.Errorf("can't resolve %s contract: %w", ctrName, err)
		}
		infos = append(infos, contractDumpInfo{name: ctrName, hash: h})
	}

	fillContractVersion(cmd, c, infos)
	fillContractExpiration(c, infos)
	printContractInfo(cmd, infos)

	return nil
}

func dumpCustomZoneHashes(cmd *cobra.Command, nnsHash util.Uint160, zone string, c Client) error {
	const nnsMaxTokens = 100

	inv := invoker.New(c, nil)
	nnsReader := nns.NewReader(inv, nnsHash)

	if !strings.HasPrefix(zone, ".") {
		zone = "." + zone
	}

	var infos []contractDumpInfo
	processItem := func(item stackitem.Item) {
		bs, err := item.TryBytes()
		if err != nil {
			cmd.PrintErrf("Invalid NNS record: %v\n", err)
			return
		}

		if !bytes.HasSuffix(bs, []byte(zone)) {
			// Related https://github.com/nspcc-dev/neofs-contract/issues/316.
			return
		}

		h, err := nnsResolveHash(nnsReader, string(bs))
		if err != nil {
			cmd.PrintErrf("Could not resolve name %s: %v\n", string(bs), err)
			return
		}

		infos = append(infos, contractDumpInfo{
			hash: h,
			name: strings.TrimSuffix(string(bs), zone),
		})
	}

	script, err := smartcontract.CreateCallAndPrefetchIteratorScript(nnsHash, "tokens", nnsMaxTokens)
	if err != nil {
		return fmt.Errorf("building prefetching script: %w", err)
	}

	items, sessionID, iter, err := unwrap.ArrayAndSessionIterator(inv.Run(script))
	if err != nil {
		if errors.Is(err, unwrap.ErrNoSessionID) {
			items, err := unwrap.Array(inv.CallAndExpandIterator(nnsHash, "tokens", nnsMaxTokens))
			if err != nil {
				return fmt.Errorf("can't get a list of NNS domains: %w", err)
			}
			if len(items) == nnsMaxTokens {
				cmd.PrintErrln("Provided RPC endpoint doesn't support sessions, some hashes might be lost.")
			}
			for i := range items {
				processItem(items[i])
			}
		} else {
			return err
		}
	} else {
		for _, item := range items {
			processItem(item)
		}

		defer func() {
			_ = inv.TerminateSession(sessionID)
		}()

		items, err := inv.TraverseIterator(sessionID, &iter, nnsMaxTokens)
		for err == nil && len(items) != 0 {
			for i := range items {
				processItem(items[i])
			}
			items, err = inv.TraverseIterator(sessionID, &iter, nnsMaxTokens)
		}
		if err != nil {
			return fmt.Errorf("error during NNS domains iteration: %w", err)
		}
	}

	fillContractVersion(cmd, c, infos)
	fillContractExpiration(c, infos)
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
		var timeStr = "unknown"
		if info.expiration != 0 {
			timeStr = time.UnixMilli(info.expiration).String()
		}
		if info.version == "" {
			info.version = "unknown"
		}
		_, _ = tw.Write([]byte(fmt.Sprintf("%s\t(%s):\t%s\t%s\n",
			info.name, info.version, info.hash.StringLE(),
			timeStr)))
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

func fillContractExpiration(c Client, infos []contractDumpInfo) {
	n11r := nep11.NewNonDivisibleReader(invoker.New(c, nil), infos[0].hash)
	for i := range infos {
		if infos[i].hash.Equals(util.Uint160{}) {
			continue
		}
		props, err := n11r.Properties([]byte(strings.ReplaceAll(infos[i].name, " ", "") + ".neofs"))
		if err != nil {
			continue // OK for NNS itself, for example.
		}
		exp, err := expirationFromProperties(props)
		if err != nil {
			continue // Should be there, but who knows.
		}
		infos[i].expiration = exp
	}
}

func expirationFromProperties(props *stackitem.Map) (int64, error) {
	elems := props.Value().([]stackitem.MapElement)
	for _, e := range elems {
		k, err := e.Key.TryBytes()
		if err != nil {
			continue
		}

		if string(k) == "expiration" {
			v, err := e.Value.TryInteger()
			if err != nil || !v.IsInt64() {
				continue
			}
			return v.Int64(), nil
		}
	}
	return 0, errors.New("not found")
}
