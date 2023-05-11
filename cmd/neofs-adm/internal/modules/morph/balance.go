package morph

import (
	"crypto/elliptic"
	"errors"
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-contract/nns"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type accBalancePair struct {
	scriptHash util.Uint160
	balance    *big.Int
}

const (
	dumpBalancesStorageFlag       = "storage"
	dumpBalancesAlphabetFlag      = "alphabet"
	dumpBalancesProxyFlag         = "proxy"
	dumpBalancesUseScriptHashFlag = "script-hash"
)

func dumpBalances(cmd *cobra.Command, _ []string) error {
	var (
		dumpStorage, _  = cmd.Flags().GetBool(dumpBalancesStorageFlag)
		dumpAlphabet, _ = cmd.Flags().GetBool(dumpBalancesAlphabetFlag)
		dumpProxy, _    = cmd.Flags().GetBool(dumpBalancesProxyFlag)
		nnsCs           *state.Contract
		nmHash          util.Uint160
	)

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return err
	}

	inv := invoker.New(c, nil)

	if dumpStorage || dumpAlphabet || dumpProxy {
		nnsCs, err = c.GetContractStateByID(1)
		if err != nil {
			return fmt.Errorf("can't get NNS contract info: %w", err)
		}

		nmHash, err = nnsResolveHash(inv, nnsCs.Hash, netmapContract+".neofs")
		if err != nil {
			return fmt.Errorf("can't get netmap contract hash: %w", err)
		}
	}

	irList, err := fetchIRNodes(c, nmHash, rolemgmt.Hash)
	if err != nil {
		return err
	}

	if err := fetchBalances(inv, gas.Hash, irList); err != nil {
		return err
	}
	printBalances(cmd, "Inner ring nodes balances:", irList)

	if dumpStorage {
		arr, err := unwrap.Array(inv.Call(nmHash, "netmap"))
		if err != nil {
			return errors.New("can't fetch the list of storage nodes")
		}

		snList := make([]accBalancePair, len(arr))
		for i := range arr {
			node, ok := arr[i].Value().([]stackitem.Item)
			if !ok || len(node) == 0 {
				return errors.New("can't parse the list of storage nodes")
			}
			bs, err := node[0].TryBytes()
			if err != nil {
				return errors.New("can't parse the list of storage nodes")
			}
			var ni netmap.NodeInfo
			if err := ni.Unmarshal(bs); err != nil {
				return fmt.Errorf("can't parse the list of storage nodes: %w", err)
			}
			pub, err := keys.NewPublicKeyFromBytes(ni.PublicKey(), elliptic.P256())
			if err != nil {
				return fmt.Errorf("can't parse storage node public key: %w", err)
			}
			snList[i].scriptHash = pub.GetScriptHash()
		}

		if err := fetchBalances(inv, gas.Hash, snList); err != nil {
			return err
		}
		printBalances(cmd, "\nStorage node balances:", snList)
	}

	if dumpProxy {
		h, err := nnsResolveHash(inv, nnsCs.Hash, proxyContract+".neofs")
		if err != nil {
			return fmt.Errorf("can't get hash of the proxy contract: %w", err)
		}

		proxyList := []accBalancePair{{scriptHash: h}}
		if err := fetchBalances(inv, gas.Hash, proxyList); err != nil {
			return err
		}
		printBalances(cmd, "\nProxy contract balance:", proxyList)
	}

	if dumpAlphabet {
		alphaList := make([]accBalancePair, len(irList))

		w := io.NewBufBinWriter()
		for i := range alphaList {
			emit.AppCall(w.BinWriter, nnsCs.Hash, "resolve", callflag.ReadOnly,
				getAlphabetNNSDomain(i),
				int64(nns.TXT))
		}
		if w.Err != nil {
			panic(w.Err)
		}

		alphaRes, err := c.InvokeScript(w.Bytes(), nil)
		if err != nil {
			return fmt.Errorf("can't fetch info from NNS: %w", err)
		}

		for i := range alphaList {
			h, err := parseNNSResolveResult(alphaRes.Stack[i])
			if err != nil {
				return fmt.Errorf("can't fetch the alphabet contract #%d hash: %w", i, err)
			}
			alphaList[i].scriptHash = h
		}

		if err := fetchBalances(inv, gas.Hash, alphaList); err != nil {
			return err
		}
		printBalances(cmd, "\nAlphabet contracts balances:", alphaList)
	}

	return nil
}

func fetchIRNodes(c Client, nmHash, desigHash util.Uint160) ([]accBalancePair, error) {
	inv := invoker.New(c, nil)

	height, err := c.GetBlockCount()
	if err != nil {
		return nil, fmt.Errorf("can't get block height: %w", err)
	}

	arr, err := getDesignatedByRole(inv, desigHash, noderoles.NeoFSAlphabet, height)
	if err != nil {
		return nil, errors.New("can't fetch list of IR nodes from the netmap contract")
	}

	var irList = make([]accBalancePair, len(arr))
	for i := range arr {
		irList[i].scriptHash = arr[i].GetScriptHash()
	}
	return irList, nil
}

func printBalances(cmd *cobra.Command, prefix string, accounts []accBalancePair) {
	useScriptHash, _ := cmd.Flags().GetBool(dumpBalancesUseScriptHashFlag)

	cmd.Println(prefix)
	for i := range accounts {
		var addr string
		if useScriptHash {
			addr = accounts[i].scriptHash.StringLE()
		} else {
			addr = address.Uint160ToString(accounts[i].scriptHash)
		}
		cmd.Printf("%s: %s\n", addr, fixedn.ToString(accounts[i].balance, 8))
	}
}

func fetchBalances(c *invoker.Invoker, gasHash util.Uint160, accounts []accBalancePair) error {
	w := io.NewBufBinWriter()
	for i := range accounts {
		emit.AppCall(w.BinWriter, gasHash, "balanceOf", callflag.ReadStates, accounts[i].scriptHash)
	}
	if w.Err != nil {
		panic(w.Err)
	}

	res, err := c.Run(w.Bytes())
	if err != nil || res.State != vmstate.Halt.String() || len(res.Stack) != len(accounts) {
		return errors.New("can't fetch account balances")
	}

	for i := range accounts {
		bal, err := res.Stack[i].TryInteger()
		if err != nil {
			return fmt.Errorf("can't parse account balance: %w", err)
		}
		accounts[i].balance = bal
	}
	return nil
}
