package fschain

import (
	"crypto/elliptic"
	"errors"
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neofs-contract/rpc/balance"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
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
		nnsReader       *nns.ContractReader
		nmHash          util.Uint160
	)

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return err
	}

	inv := invoker.New(c, nil)

	if dumpStorage || dumpAlphabet || dumpProxy {
		nnsReader, err = nns.NewInferredReader(c, inv)
		if err != nil {
			return fmt.Errorf("can't find NNS contract: %w", err)
		}

		nmHash, err = nnsReader.ResolveFSContract(nns.NameNetmap)
		if err != nil {
			return fmt.Errorf("can't get netmap contract hash: %w", err)
		}
	}

	irList, err := fetchIRNodes(c, rolemgmt.Hash)
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
		h, err := nnsReader.ResolveFSContract(nns.NameProxy)
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

		for i := range alphaList {
			h, err := nnsReader.ResolveFSContract(getAlphabetNNSDomain(i))
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

func fetchIRNodes(c Client, desigHash util.Uint160) ([]accBalancePair, error) {
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
	b := smartcontract.NewBuilder()
	for i := range accounts {
		b.InvokeMethod(gasHash, "balanceOf", accounts[i].scriptHash)
	}

	script, err := b.Script()
	if err != nil {
		return fmt.Errorf("reading balances script: %w", err)
	}

	res, err := c.Run(script)
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

func depositGas(cmd *cobra.Command, gasAmount int64, receiver util.Uint160, txHash util.Uint256) error {
	wCtx, err := newInitializeContext(cmd, viper.GetViper())
	if err != nil {
		return err
	}

	nnsReader, err := nns.NewInferredReader(wCtx.Client, wCtx.ReadOnlyInvoker)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}
	bHash, err := nnsReader.ResolveFSContract(nns.NameBalance)
	if err != nil {
		return fmt.Errorf("can't find balance contract: %w", err)
	}
	a, err := actor.New(wCtx.Client, []actor.SignerAccount{{
		Signer: transaction.Signer{
			Account: wCtx.ConsensusAcc.Contract.ScriptHash(),
			Scopes:  transaction.Global, // Used for test invocations only, safe to be this way.
		},
		Account: wCtx.ConsensusAcc,
	}})
	if err != nil {
		return fmt.Errorf("can't init consensus actor: %w", err)
	}
	b := balance.New(a, bHash)

	tx, err := b.MintUnsigned(receiver, big.NewInt(gasAmount), txHash.BytesBE())
	if err != nil {
		return err
	}

	if err := wCtx.multiSignAndSend(tx, consensusAccountName); err != nil {
		return err
	}

	return wCtx.awaitTx()
}
