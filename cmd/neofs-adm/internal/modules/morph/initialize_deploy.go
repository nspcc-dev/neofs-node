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
	"github.com/spf13/viper"
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

const (
	netmapEpochKey                 = "EpochDuration"
	netmapMaxObjectSizeKey         = "MaxObjectSize"
	netmapAuditFeeKey              = "AuditFee"
	netmapContainerFeeKey          = "ContainerFee"
	netmapEigenTrustIterationsKey  = "EigenTrustIterations"
	netmapEigenTrustAlphaKey       = "EigenTrustAlpha"
	netmapBasicIncomeRateKey       = "BasicIncomeRate"
	netmapInnerRingCandidateFeeKey = "InnerRingCandidateFee"
	netmapWithdrawFeeKey           = "WithdrawFee"

	defaultEigenTrustIterations = 4
	defaultEigenTrustAlpha      = "0.1"
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
	cs, err := c.readContract(nnsContract)
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

	mgmtHash := c.nativeHash(nativenames.Management)
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

func (c *initializeContext) deployContracts(method string) error {
	mgmtHash := c.nativeHash(nativenames.Management)
	sender := c.CommitteeAcc.Contract.ScriptHash()
	for _, ctrName := range contractList {
		cs, err := c.readContract(ctrName)
		if err != nil {
			return err
		}
		cs.Hash = state.CreateContractHash(sender, cs.NEF.Checksum, cs.Manifest.Name)
	}

	alphaCs, err := c.readContract(alphabetContract)
	if err != nil {
		return err
	}

	nnsCs, err := c.Client.GetContractStateByID(1)
	if err != nil {
		return err
	}
	nnsHash := nnsCs.Hash

	var keysParam []smartcontract.Parameter

	// alphabet contracts should be deployed by individual nodes to get different hashes.
	for i, acc := range c.Accounts {
		ctrHash := state.CreateContractHash(acc.Contract.ScriptHash(), alphaCs.NEF.Checksum, alphaCs.Manifest.Name)
		if _, err := c.Client.GetContractStateByHash(ctrHash); err == nil {
			c.Command.Printf("Alphabet contract #%d is already deployed.\n", i)
			continue
		}

		invokeHash := mgmtHash
		if method == "migrate" {
			invokeHash, err = nnsResolveHash(c.Client, nnsHash, getAlphabetNNSDomain(i))
			if err != nil {
				return fmt.Errorf("can't resolve hash for contract update: %w", err)
			}
		}

		keysParam = append(keysParam, smartcontract.Parameter{
			Type:  smartcontract.PublicKeyType,
			Value: acc.PrivateKey().PublicKey().Bytes(),
		})

		params := getContractDeployParameters(alphaCs.RawNEF, alphaCs.RawManifest,
			c.getAlphabetDeployParameters(i, len(c.Wallets)))
		signer := transaction.Signer{
			Account: acc.Contract.ScriptHash(),
			Scopes:  transaction.CalledByEntry,
		}
		res, err := c.Client.InvokeFunction(invokeHash, method, params, []transaction.Signer{signer})
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
			c.Command.Printf("%s contract is already deployed.\n", ctrName)
			continue
		}

		invokeHash := mgmtHash
		if method == "migrate" {
			invokeHash, err = nnsResolveHash(c.Client, nnsHash, ctrName+".neofs")
			if err != nil {
				return fmt.Errorf("can't resolve hash for contract update: %w", err)
			}
		}

		params := getContractDeployParameters(cs.RawNEF, cs.RawManifest,
			c.getContractDeployData(ctrName, keysParam))
		signer := transaction.Signer{
			Account: c.CommitteeAcc.Contract.ScriptHash(),
			Scopes:  transaction.CalledByEntry,
		}

		res, err := c.Client.InvokeFunction(invokeHash, method, params, []transaction.Signer{signer})
		if err != nil {
			return fmt.Errorf("can't deploy contract: %w", err)
		}

		if err := c.sendCommitteeTx(res.Script, res.GasConsumed); err != nil {
			return err
		}
	}

	return c.awaitTx()
}

func (c *initializeContext) readContract(ctrName string) (*contractState, error) {
	if cs, ok := c.Contracts[ctrName]; ok {
		return cs, nil
	}

	rawNef, err := ioutil.ReadFile(path.Join(c.ContractPath, ctrName, ctrName+"_contract.nef"))
	if err != nil {
		return nil, fmt.Errorf("can't read NEF file: %w", err)
	}
	nf, err := nef.FileFromBytes(rawNef)
	if err != nil {
		return nil, fmt.Errorf("can't parse NEF file: %w", err)
	}
	rawManif, err := ioutil.ReadFile(path.Join(c.ContractPath, ctrName, "config.json"))
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
	items := make([]smartcontract.Parameter, 1, 6)
	items[0] = newContractParameter(smartcontract.BoolType, false) // notaryDisabled is false

	switch ctrName {
	case neofsContract:
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[processingContract].Hash),
			newContractParameter(smartcontract.ArrayType, keysParam),
			newContractParameter(smartcontract.ArrayType, smartcontract.Parameter{}))
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
		configParam := []smartcontract.Parameter{
			{Type: smartcontract.StringType, Value: netmapEpochKey},
			{Type: smartcontract.IntegerType, Value: viper.GetInt64(epochDurationInitFlag)},
			{Type: smartcontract.StringType, Value: netmapMaxObjectSizeKey},
			{Type: smartcontract.IntegerType, Value: viper.GetInt64(maxObjectSizeInitFlag)},
			{Type: smartcontract.StringType, Value: netmapAuditFeeKey},
			{Type: smartcontract.IntegerType, Value: viper.GetInt64(auditFeeInitFlag)},
			{Type: smartcontract.StringType, Value: netmapContainerFeeKey},
			{Type: smartcontract.IntegerType, Value: viper.GetInt64(containerFeeInitFlag)},
			{Type: smartcontract.StringType, Value: netmapEigenTrustIterationsKey},
			{Type: smartcontract.IntegerType, Value: int64(defaultEigenTrustIterations)},
			{Type: smartcontract.StringType, Value: netmapEigenTrustAlphaKey},
			{Type: smartcontract.StringType, Value: defaultEigenTrustAlpha},
			{Type: smartcontract.StringType, Value: netmapBasicIncomeRateKey},
			{Type: smartcontract.IntegerType, Value: viper.GetInt64(incomeRateInitFlag)},
			{Type: smartcontract.StringType, Value: netmapInnerRingCandidateFeeKey},
			{Type: smartcontract.IntegerType, Value: viper.GetInt64(candidateFeeInitFlag)},
			{Type: smartcontract.StringType, Value: netmapWithdrawFeeKey},
			{Type: smartcontract.IntegerType, Value: viper.GetInt64(withdrawFeeInitFlag)},
		}
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[balanceContract].Hash),
			newContractParameter(smartcontract.Hash160Type, c.Contracts[containerContract].Hash),
			newContractParameter(smartcontract.ArrayType, keysParam),
			newContractParameter(smartcontract.ArrayType, configParam))
	case proxyContract:
		items = []smartcontract.Parameter{
			newContractParameter(smartcontract.Hash160Type, c.Contracts[netmapContract].Hash),
		}
	case reputationContract:
	default:
		panic(fmt.Sprintf("invalid contract name: %s", ctrName))
	}
	return items
}

func (c *initializeContext) getAlphabetDeployParameters(i, n int) []smartcontract.Parameter {
	return []smartcontract.Parameter{
		newContractParameter(smartcontract.BoolType, false),
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
