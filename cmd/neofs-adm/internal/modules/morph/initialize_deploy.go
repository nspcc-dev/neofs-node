package morph

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
)

const (
	nnsContract        = "nns"
	neofsContract      = "neofs"      // not deployed in side-chain.
	processingContract = "processing" // not deployed in side-chain.
	alphabetContract   = "alphabet"
	auditContract      = "audit"
	balanceContract    = "balance"
	containerContract  = "container"
	neofsIDContract    = "neofsid"
	netmapContract     = "netmap"
	proxyContract      = "proxy"
	reputationContract = "reputation"
)

var contractList = []string{
	auditContract,
	balanceContract,
	containerContract,
	neofsIDContract,
	netmapContract,
	proxyContract,
	reputationContract,
}

type contractState struct {
	NEF         *nef.File
	RawNEF      []byte
	Manifest    *manifest.Manifest
	RawManifest []byte
	Hash        util.Uint160
}

func (c *initializeContext) deployNNS() error {
	ctrPath, err := c.Command.Flags().GetString(contractsInitFlag)
	if err != nil {
		return fmt.Errorf("missing contracts path: %w", err)
	}

	cs, err := c.readContract(ctrPath, nnsContract)
	if err != nil {
		return err
	}

	h := state.CreateContractHash(c.CommitteeAcc.Contract.ScriptHash(), cs.NEF.Checksum, cs.Manifest.Name)
	if _, err := c.Client.GetContractStateByHash(h); err == nil {
		return nil
	}

	params := getContractDeployParameters(cs.RawNEF, cs.RawManifest, nil)
	signer := transaction.Signer{
		Account: c.CommitteeAcc.Contract.ScriptHash(),
		Scopes:  transaction.CalledByEntry,
	}

	mgmtHash, _ := c.Client.GetNativeContractHash(nativenames.Management)
	res, err := c.Client.InvokeFunction(mgmtHash, "deploy", params, []transaction.Signer{signer})
	if err != nil {
		return fmt.Errorf("can't deploy contract: %w", err)
	}

	tx, err := c.Client.CreateTxFromScript(res.Script, c.CommitteeAcc, res.GasConsumed, 0, []client.SignerAccount{{
		Signer:  signer,
		Account: c.CommitteeAcc,
	}})
	if err != nil {
		return fmt.Errorf("failed to create deploy tx for %s: %w", nnsContract, err)
	}

	if err := c.multiSignAndSend(tx, committeeAccountName); err != nil {
		return fmt.Errorf("can't send deploy transaction: %w", err)
	}

	return c.awaitTx()
}

func (c *initializeContext) deployContracts() error {
	ctrPath, err := c.Command.Flags().GetString(contractsInitFlag)
	if err != nil {
		return fmt.Errorf("missing contracts path: %w", err)
	}

	mgmtHash, _ := c.Client.GetNativeContractHash(nativenames.Management)
	sender := c.CommitteeAcc.Contract.ScriptHash()
	for _, ctrName := range contractList {
		cs, err := c.readContract(ctrPath, ctrName)
		if err != nil {
			return err
		}
		cs.Hash = state.CreateContractHash(sender, cs.NEF.Checksum, cs.Manifest.Name)
	}

	var keysParam []smartcontract.Parameter

	// alphabet contracts should be deployed by individual nodes to get different hashes.
	for i, w := range c.Wallets {
		acc, err := getWalletAccount(w, singleAccountName)
		if err != nil {
			return err
		}

		cs, err := c.readContract(ctrPath, alphabetContract)
		if err != nil {
			return err
		}

		ctrHash := state.CreateContractHash(acc.Contract.ScriptHash(), cs.NEF.Checksum, cs.Manifest.Name)
		if _, err := c.Client.GetContractStateByHash(ctrHash); err == nil {
			c.Command.Printf("Stage 4: alphabet contract #%d is already deployed.\n", i)
			continue
		}

		keysParam = append(keysParam, smartcontract.Parameter{
			Type:  smartcontract.PublicKeyType,
			Value: acc.PrivateKey().PublicKey().Bytes(),
		})

		params := getContractDeployParameters(cs.RawNEF, cs.RawManifest,
			c.getAlphabetDeployParameters(i, len(c.Wallets)))
		signer := transaction.Signer{
			Account: acc.Contract.ScriptHash(),
			Scopes:  transaction.CalledByEntry,
		}
		res, err := c.Client.InvokeFunction(mgmtHash, "deploy", params, []transaction.Signer{signer})
		if err != nil {
			return fmt.Errorf("can't deploy alphabet #%d contract: %w", i, err)
		}
		h, err := c.Client.SignAndPushInvocationTx(res.Script, acc, -1, 0, []client.SignerAccount{{
			Signer:  signer,
			Account: acc,
		}})
		if err != nil {
			return fmt.Errorf("can't push deploy transaction: %w", err)
		}

		c.Hashes = append(c.Hashes, h)
	}

	for _, ctrName := range contractList {
		cs := c.Contracts[ctrName]
		if _, err := c.Client.GetContractStateByHash(cs.Hash); err == nil {
			c.Command.Printf("Stage 4: %s contract is already deployed.\n", ctrName)
			continue
		}

		params := getContractDeployParameters(cs.RawNEF, cs.RawManifest,
			c.getContractDeployData(ctrName, keysParam))
		signer := transaction.Signer{
			Account: c.CommitteeAcc.Contract.ScriptHash(),
			Scopes:  transaction.CalledByEntry,
		}

		res, err := c.Client.InvokeFunction(mgmtHash, "deploy", params, []transaction.Signer{signer})
		if err != nil {
			return fmt.Errorf("can't deploy contract: %w", err)
		}

		if err := c.sendCommitteeTx(res.Script, res.GasConsumed); err != nil {
			return err
		}
	}

	return c.awaitTx()
}

func (c *initializeContext) readContract(ctrPath, ctrName string) (*contractState, error) {
	if cs, ok := c.Contracts[ctrName]; ok {
		return cs, nil
	}

	rawNef, err := ioutil.ReadFile(path.Join(ctrPath, ctrName, ctrName+"_contract.nef"))
	if err != nil {
		return nil, fmt.Errorf("can't read NEF file: %w", err)
	}
	nf, err := nef.FileFromBytes(rawNef)
	if err != nil {
		return nil, fmt.Errorf("can't parse NEF file: %w", err)
	}
	rawManif, err := ioutil.ReadFile(path.Join(ctrPath, ctrName, "config.json"))
	if err != nil {
		return nil, fmt.Errorf("can't read manifest file: %w", err)
	}
	m := new(manifest.Manifest)
	if err := json.Unmarshal(rawManif, m); err != nil {
		return nil, fmt.Errorf("can't parse manifest file: %w", err)
	}

	c.Contracts[ctrName] = &contractState{
		NEF:         &nf,
		RawNEF:      rawNef,
		Manifest:    m,
		RawManifest: rawManif,
	}
	return c.Contracts[ctrName], nil
}

func getContractDeployParameters(rawNef, rawManif []byte, deployData []smartcontract.Parameter) []smartcontract.Parameter {
	return []smartcontract.Parameter{
		{
			Type:  smartcontract.ByteArrayType,
			Value: rawNef,
		},
		{
			Type:  smartcontract.ByteArrayType,
			Value: rawManif,
		},
		{
			Type:  smartcontract.ArrayType,
			Value: deployData,
		},
	}
}

func (c *initializeContext) getContractDeployData(ctrName string, keysParam []smartcontract.Parameter) []smartcontract.Parameter {
	items := make([]smartcontract.Parameter, 2, 7)
	items[0] = newContractParameter(smartcontract.BoolType, false)                                   // notaryDisabled is false
	items[1] = newContractParameter(smartcontract.Hash160Type, c.CommitteeAcc.Contract.ScriptHash()) // owner is committee

	switch ctrName {
	case neofsContract:
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[processingContract].Hash),
			newContractParameter(smartcontract.ArrayType, keysParam))
	case processingContract:
		items = append(items, newContractParameter(smartcontract.Hash160Type, c.Contracts[neofsContract].Hash))
		return items[1:] // no notary info
	case auditContract:
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[netmapContract].Hash))
	case balanceContract:
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[netmapContract].Hash),
			newContractParameter(smartcontract.Hash160Type, c.Contracts[containerContract].Hash))
	case containerContract:
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[netmapContract].Hash),
			newContractParameter(smartcontract.Hash160Type, c.Contracts[balanceContract].Hash),
			newContractParameter(smartcontract.Hash160Type, c.Contracts[neofsIDContract].Hash))
	case neofsIDContract:
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[netmapContract].Hash),
			newContractParameter(smartcontract.Hash160Type, c.Contracts[containerContract].Hash))
	case netmapContract:
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[balanceContract].Hash),
			newContractParameter(smartcontract.Hash160Type, c.Contracts[containerContract].Hash),
			newContractParameter(smartcontract.ArrayType, keysParam))
	case proxyContract:
		items = append(items, newContractParameter(smartcontract.Hash160Type, c.Contracts[netmapContract].Hash))
	case reputationContract:
	default:
		panic(fmt.Sprintf("invalid contract name: %s", ctrName))
	}
	return items
}

func (c *initializeContext) getAlphabetDeployParameters(i, n int) []smartcontract.Parameter {
	return []smartcontract.Parameter{
		newContractParameter(smartcontract.BoolType, false),
		newContractParameter(smartcontract.Hash160Type, c.CommitteeAcc.Contract.ScriptHash()),
		newContractParameter(smartcontract.Hash160Type, c.Contracts[netmapContract].Hash),
		newContractParameter(smartcontract.Hash160Type, c.Contracts[proxyContract].Hash),
		newContractParameter(smartcontract.StringType, innerring.GlagoliticLetter(i).String()),
		newContractParameter(smartcontract.IntegerType, int64(i)),
		newContractParameter(smartcontract.IntegerType, int64(n)),
	}
}

func newContractParameter(typ smartcontract.ParamType, value interface{}) smartcontract.Parameter {
	return smartcontract.Parameter{
		Type:  typ,
		Value: value,
	}
}
