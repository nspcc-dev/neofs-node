package fschain

import (
	"crypto/elliptic"
	"errors"
	"fmt"
	"math/big"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/config"
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
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// balanceSubcommand groups operations with balance contract, WIP.
var balanceSubcommand = &cobra.Command{
	Use:   "balance",
	Short: "Operations with balance contract",
	PersistentPreRun: func(cmd *cobra.Command, _ []string) {
		_ = viper.BindPFlag(endpointFlag, cmd.Flags().Lookup(endpointFlag))
	},
	Args: cobra.NoArgs,
}

var (
	balanceContainerStatusCmd = &cobra.Command{
		Use:     "container-status",
		Example: "container-status -- [<cID>]",
		Short:   "Check container billing status, if <value> is missing, checks every container",
		RunE:    balanceContainerPayment,
		Args:    cobra.RangeArgs(0, 1),
	}
)

func balanceContainerPayment(cmd *cobra.Command, args []string) error {
	var cID cid.ID
	if len(args) > 0 {
		err := cID.DecodeString(args[0])
		if err != nil {
			return fmt.Errorf("invalid container ID: %w", err)
		}
	}

	c, err := getN3Client(viper.GetViper())
	if err != nil {
		return fmt.Errorf("can't create N3 client: %w", err)
	}

	inv := invoker.New(c, nil)
	nnsReader, err := nns.NewInferredReader(c, inv)
	if err != nil {
		return fmt.Errorf("can't find NNS contract: %w", err)
	}
	balanceHash, err := nnsReader.ResolveFSContract(nns.NameBalance)
	if err != nil {
		return fmt.Errorf("balance contract hash resolution: %w", err)
	}
	balanceReader := balance.NewReader(inv, balanceHash)

	if cID.IsZero() {
		sID, iter, err := balanceReader.IterateUnpaid()
		if err != nil {
			return fmt.Errorf("failed to open session: %w", err)
		}
		defer func() {
			if sID != uuid.Nil {
				_ = inv.TerminateSession(sID)
			}
		}()

		for {
			ii, err := inv.TraverseIterator(sID, &iter, config.DefaultMaxIteratorResultItems)
			if err != nil {
				return fmt.Errorf("iterator traversal; session: %s, error: %w", sID, err)
			}
			if len(ii) == 0 {
				break
			}

			for _, i := range ii {
				kv := i.Value().([]stackitem.Item)

				cidRaw, err := kv[0].TryBytes()
				if err != nil {
					return fmt.Errorf("container ID is not bytes: %w", err)
				}
				cID, err = cid.DecodeBytes(cidRaw)
				if err != nil {
					return fmt.Errorf("invalid container ID in iterator: %w", err)
				}

				unpaidSince, err := kv[1].TryInteger()
				if err != nil {
					return fmt.Errorf("unpaid epoch is not an int: %w", err)
				}

				printContainerUnpaidStatus(cmd, cID, unpaidSince.Int64())
			}
		}
	} else {
		unpaidSince, err := balanceReader.GetUnpaidContainerEpoch(util.Uint256(cID))
		if err != nil {
			return fmt.Errorf("rpc call: %w", err)
		}
		printContainerUnpaidStatus(cmd, cID, unpaidSince.Int64())
	}

	return nil
}

func printContainerUnpaidStatus(cmd *cobra.Command, cID cid.ID, unpaidSince int64) {
	if unpaidSince < 0 {
		cmd.Printf("%s: container is paid\n", cID)
	} else {
		cmd.Printf("%s: container is unpaid since %d epoch\n", cID, unpaidSince)
	}
}

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
			return fmt.Errorf("can't fetch the list of storage nodes: %w", err)
		}

		snList := make([]accBalancePair, len(arr))
		for i := range arr {
			node, ok := arr[i].Value().([]stackitem.Item)
			if !ok || len(node) == 0 {
				return errors.New("can't parse the list of storage nodes")
			}
			bs, err := node[0].TryBytes()
			if err != nil {
				return fmt.Errorf("can't parse the list of storage nodes: %w", err)
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

var errGetDesignatedByRoleResponse = errors.New("`getDesignatedByRole`: invalid response")

func getDesignatedByRole(inv *invoker.Invoker, h util.Uint160, role noderoles.Role, u uint32) (keys.PublicKeys, error) {
	arr, err := unwrap.Array(inv.Call(h, "getDesignatedByRole", int64(role), int64(u)))
	if err != nil {
		return nil, errGetDesignatedByRoleResponse
	}

	pubs := make(keys.PublicKeys, len(arr))
	for i := range arr {
		bs, err := arr[i].TryBytes()
		if err != nil {
			return nil, errGetDesignatedByRoleResponse
		}
		pubs[i], err = keys.NewPublicKeyFromBytes(bs, elliptic.P256())
		if err != nil {
			return nil, errGetDesignatedByRoleResponse
		}
	}

	return pubs, nil
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
