package morph

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"text/tabwriter"

	nns "github.com/nspcc-dev/neo-go/examples/nft-nd-nns"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const lastGlagoliticLetter = 41

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

	irSize := 0
	for ; irSize < lastGlagoliticLetter; irSize++ {
		ok, err := c.NNSIsAvailable(cs.Hash, getAlphabetNNSDomain(irSize))
		if err != nil {
			return err
		} else if ok {
			break
		}
	}

	buf := bytes.NewBuffer(nil)
	tw := tabwriter.NewWriter(buf, 0, 2, 2, ' ', 0)

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
			ctrHash := "hash is invalid"
			bs, err := alphaRes.Stack[i].TryBytes()
			if err == nil {
				ctrHash = string(bs) // hashes are stored as hex-encoded LE string
			}

			_, _ = tw.Write([]byte(fmt.Sprintf("alphabet %d:\t%s\n", i, ctrHash)))
		}
	}

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

func dumpNetworkConfig(cmd *cobra.Command, _ []string) error {
	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't to initialize context: %w", err)
	}

	cs, err := wCtx.Client.GetContractStateByID(1)
	if err != nil {
		return fmt.Errorf("can't get NNS contract info: %w", err)
	}

	res, err := wCtx.Client.InvokeFunction(cs.Hash, "resolve", []smartcontract.Parameter{
		{Type: smartcontract.StringType, Value: netmapContract + ".neofs"},
		{Type: smartcontract.IntegerType, Value: int64(nns.TXT)},
	}, nil)
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}
	if len(res.Stack) == 0 {
		return errors.New("empty response from NNS")
	}

	var nmHash util.Uint160
	bs, err := res.Stack[0].TryBytes()
	if err == nil {
		nmHash, err = util.Uint160DecodeStringLE(string(bs))
	}
	if err != nil {
		return fmt.Errorf("invalid response from NNS contract: %w", err)
	}

	res, err = wCtx.Client.InvokeFunction(nmHash, "listConfig",
		[]smartcontract.Parameter{}, []transaction.Signer{{}})
	if err != nil || res.State != vm.HaltState.String() || len(res.Stack) == 0 {
		return errors.New("can't fetch list of network config keys from the netmap contract")
	}

	arr, ok := res.Stack[0].Value().([]stackitem.Item)
	if !ok {
		return errors.New("invalid ListConfig response from netmap contract")
	}

	buf := bytes.NewBuffer(nil)
	tw := tabwriter.NewWriter(buf, 0, 2, 2, ' ', 0)

	for _, param := range arr {
		tuple, ok := param.Value().([]stackitem.Item)
		if !ok || len(tuple) != 2 {
			return errors.New("invalid ListConfig response from netmap contract")
		}

		k, err := tuple[0].TryBytes()
		if err != nil {
			return errors.New("invalid config key from netmap contract")
		}

		v, err := tuple[1].TryBytes()
		if err != nil {
			return errors.New("invalid config value from netmap contract")
		}

		switch string(k) {
		case netmapAuditFeeKey, netmapBasicIncomeRateKey,
			netmapContainerFeeKey, netmapEigenTrustIterationsKey,
			netmapEpochKey, netmapInnerRingCandidateFeeKey,
			netmapMaxObjectSizeKey, netmapWithdrawFeeKey:
			nbuf := make([]byte, 8)
			copy(nbuf[:], v)
			n := binary.LittleEndian.Uint64(nbuf)
			_, _ = tw.Write([]byte(fmt.Sprintf("%s:\t%d (int)\n", k, n)))
		case netmapEigenTrustAlphaKey:
			_, _ = tw.Write([]byte(fmt.Sprintf("%s:\t%s (str)\n", k, v)))
		default:
			_, _ = tw.Write([]byte(fmt.Sprintf("%s:\t%s (hex)\n", k, hex.EncodeToString(v))))
		}
	}

	_ = tw.Flush()
	cmd.Print(buf.String())

	return nil
}
