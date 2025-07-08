package fschain

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const forceConfigSet = "force"

func dumpNetworkConfig(cmd *cobra.Command, _ []string) error {
	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	inv := invoker.New(c, nil)

	nnsReader, err := nns.NewInferredReader(c, inv)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}

	nmHash, err := nnsReader.ResolveFSContract(nns.NameNetmap)
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}

	arr, err := unwrap.Array(inv.Call(nmHash, "listConfig"))
	if err != nil {
		return fmt.Errorf("can't fetch list of network config keys from the netmap contract: %w", err)
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
			return fmt.Errorf("invalid config key from netmap contract: %w", err)
		}

		v, err := tuple[1].TryBytes()
		if err != nil {
			return invalidConfigValueErr(k)
		}

		switch string(k) {
		case netmapBasicIncomeRateKey,
			netmapContainerFeeKey, netmapContainerAliasFeeKey,
			netmapEigenTrustIterationsKey,
			netmapEpochKey, netmapInnerRingCandidateFeeKey,
			netmapMaxObjectSizeKey, netmapWithdrawFeeKey:
			nbuf := make([]byte, 8)
			copy(nbuf, v)
			n := binary.LittleEndian.Uint64(nbuf)
			_, _ = tw.Write([]byte(fmt.Sprintf("%s:\t%d (int)\n", k, n)))
		case netmapEigenTrustAlphaKey:
			_, _ = tw.Write([]byte(fmt.Sprintf("%s:\t%s (str)\n", k, v)))
		case netmapHomomorphicHashDisabledKey:
			vBool, err := tuple[1].TryBool()
			if err != nil {
				return invalidConfigValueErr(k)
			}

			_, _ = tw.Write([]byte(fmt.Sprintf("%s:\t%t (bool)\n", k, vBool)))
		default:
			_, _ = tw.Write([]byte(fmt.Sprintf("%s:\t%s (hex)\n", k, hex.EncodeToString(v))))
		}
	}

	_ = tw.Flush()
	cmd.Print(buf.String())

	return nil
}

func setConfigCmd(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return errors.New("empty config pairs")
	}

	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't initialize context: %w", err)
	}

	nnsReader, err := nns.NewInferredReader(wCtx.Client, wCtx.ReadOnlyInvoker)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}

	nmHash, err := nnsReader.ResolveFSContract(nns.NameNetmap)
	if err != nil {
		return fmt.Errorf("can't get netmap contract hash: %w", err)
	}

	forceFlag, _ := cmd.Flags().GetBool(forceConfigSet)

	b := smartcontract.NewBuilder()
	for _, arg := range args {
		k, v, err := parseConfigPair(arg, forceFlag)
		if err != nil {
			return err
		}

		// In NeoFS this is done via Notary contract. Here, however, we can form the
		// transaction locally. The first `nil` argument is required only for notary
		// disabled environment which is not supported by that command.
		b.InvokeMethod(nmHash, "setConfig", nil, k, v)
	}

	script, err := b.Script()
	if err != nil {
		return fmt.Errorf("config setting script: %w", err)
	}

	err = wCtx.sendConsensusTx(script)
	if err != nil {
		return err
	}

	return wCtx.awaitTx()
}

func parseConfigPair(kvStr string, force bool) (key string, val any, err error) {
	kv := strings.SplitN(kvStr, "=", 2)
	if len(kv) != 2 {
		return "", nil, fmt.Errorf("invalid parameter format: must be 'key=val', got: %s", kvStr)
	}

	key = kv[0]
	valRaw := kv[1]

	switch key {
	case netmapBasicIncomeRateKey,
		netmapContainerFeeKey, netmapContainerAliasFeeKey,
		netmapEigenTrustIterationsKey,
		netmapEpochKey, netmapInnerRingCandidateFeeKey,
		netmapMaxObjectSizeKey, netmapWithdrawFeeKey:
		val, err = strconv.ParseInt(valRaw, 10, 64)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected int, got '%s'", key, valRaw)
		}
	case netmapEigenTrustAlphaKey:
		// just check that it could
		// be parsed correctly
		_, err = strconv.ParseFloat(kv[1], 64)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected float, got '%s'", key, valRaw)
		}

		val = valRaw
	case netmapHomomorphicHashDisabledKey:
		val, err = strconv.ParseBool(valRaw)
		if err != nil {
			err = fmt.Errorf("invalid value for %s key, expected bool, got '%s'", key, valRaw)
		}

	default:
		if !force {
			return "", nil, fmt.Errorf(
				"'%s' key is not well-known, use '--%s' flag if want to set it anyway",
				key, forceConfigSet)
		}

		val = valRaw
	}

	return
}

func invalidConfigValueErr(key []byte) error {
	return fmt.Errorf("invalid %s config value from netmap contract", key)
}
