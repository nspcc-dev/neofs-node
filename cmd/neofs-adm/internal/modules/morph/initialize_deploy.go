package morph

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
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
	netmapContainerAliasFeeKey     = "ContainerAliasFee"
	netmapEigenTrustIterationsKey  = "EigenTrustIterations"
	netmapEigenTrustAlphaKey       = "EigenTrustAlpha"
	netmapBasicIncomeRateKey       = "BasicIncomeRate"
	netmapInnerRingCandidateFeeKey = "InnerRingCandidateFee"
	netmapWithdrawFeeKey           = "WithdrawFee"

	defaultEigenTrustIterations = 4
	defaultEigenTrustAlpha      = "0.1"
)

var (
	contractList = []string{
		auditContract,
		balanceContract,
		containerContract,
		neofsIDContract,
		netmapContract,
		proxyContract,
		reputationContract,
	}

	fullContractList = append([]string{
		neofsContract,
		processingContract,
		nnsContract,
		alphabetContract,
	}, contractList...)
)

type contractState struct {
	NEF         *nef.File
	RawNEF      []byte
	Manifest    *manifest.Manifest
	RawManifest []byte
	Hash        util.Uint160
}

const updateMethodName = "update"

func (c *initializeContext) deployNNS(method string) error {
	cs := c.getContract(nnsContract)

	realCs, err := c.Client.GetContractStateByID(1)
	if err == nil && realCs.NEF.Checksum == cs.NEF.Checksum {
		c.Command.Println("NNS contract is already deployed.")
		return nil
	}

	params := getContractDeployParameters(cs.RawNEF, cs.RawManifest, nil)
	if method == updateMethodName {
		params = params[:len(params)-1] // update has only NEF and manifest args
	}

	signer := transaction.Signer{
		Account: c.CommitteeAcc.Contract.ScriptHash(),
		Scopes:  transaction.CalledByEntry,
	}

	mgmtHash := c.nativeHash(nativenames.Management)
	if method == updateMethodName {
		nnsCs, err := c.Client.GetContractStateByID(1)
		if err != nil {
			return fmt.Errorf("can't resolve NNS hash for contract update: %w", err)
		}
		mgmtHash = nnsCs.Hash
	}

	res, err := c.Client.InvokeFunction(mgmtHash, method, params, []transaction.Signer{signer})
	if err != nil {
		return fmt.Errorf("can't deploy NNS contract: %w", err)
	}
	if res.State != vm.HaltState.String() {
		return fmt.Errorf("can't deploy NNS contract: %s", res.FaultException)
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
	alphaCs := c.getContract(alphabetContract)

	nnsCs, err := c.Client.GetContractStateByID(1)
	if err != nil {
		return err
	}
	nnsHash := nnsCs.Hash

	var keysParam []smartcontract.Parameter

	// alphabet contracts should be deployed by individual nodes to get different hashes.
	for i, acc := range c.Accounts {
		ctrHash := state.CreateContractHash(acc.Contract.ScriptHash(), alphaCs.NEF.Checksum, alphaCs.Manifest.Name)
		if method == updateMethodName {
			ctrHash, err = nnsResolveHash(c.Client, nnsHash, getAlphabetNNSDomain(i))
			if err != nil {
				return fmt.Errorf("can't resolve hash for contract update: %w", err)
			}
		}
		if c.isUpdated(ctrHash, alphaCs) {
			c.Command.Printf("Alphabet contract #%d is already deployed.\n", i)
			continue
		}

		invokeHash := mgmtHash
		if method == updateMethodName {
			invokeHash = ctrHash
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
		if method == updateMethodName {
			signer = transaction.Signer{
				Account: c.CommitteeAcc.Contract.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			}
		}

		res, err := c.Client.InvokeFunction(invokeHash, method, params, []transaction.Signer{signer})
		if err != nil {
			return fmt.Errorf("can't deploy alphabet #%d contract: %w", i, err)
		}
		if res.State != vm.HaltState.String() {
			return fmt.Errorf("can't deploy alpabet #%d contract: %s", i, res.FaultException)
		}

		if method == updateMethodName {
			if err := c.sendCommitteeTx(res.Script, res.GasConsumed); err != nil {
				return err
			}
		} else {
			h, err := c.Client.SignAndPushInvocationTx(res.Script, acc, -1, 0, []client.SignerAccount{{
				Signer:  signer,
				Account: acc,
			}})
			if err != nil {
				return fmt.Errorf("can't push deploy transaction: %w", err)
			}

			c.Hashes = append(c.Hashes, h)
		}
	}

	for _, ctrName := range contractList {
		cs := c.Contracts[ctrName]

		ctrHash := cs.Hash
		if method == updateMethodName {
			ctrHash, err = nnsResolveHash(c.Client, nnsHash, ctrName+".neofs")
			if err != nil {
				return fmt.Errorf("can't resolve hash for contract update: %w", err)
			}
		}
		if c.isUpdated(ctrHash, cs) {
			c.Command.Printf("%s contract is already deployed.\n", ctrName)
			continue
		}

		invokeHash := mgmtHash
		if method == updateMethodName {
			invokeHash = ctrHash
		}

		params := getContractDeployParameters(cs.RawNEF, cs.RawManifest,
			c.getContractDeployData(ctrName, keysParam))
		signer := transaction.Signer{
			Account: c.CommitteeAcc.Contract.ScriptHash(),
			Scopes:  transaction.Global,
		}

		res, err := c.Client.InvokeFunction(invokeHash, method, params, []transaction.Signer{signer})
		if err != nil {
			return fmt.Errorf("can't deploy %s contract: %w", ctrName, err)
		}
		if res.State != vm.HaltState.String() {
			return fmt.Errorf("can't deploy %s contract: %s", ctrName, res.FaultException)
		}

		if err := c.sendCommitteeTx(res.Script, res.GasConsumed); err != nil {
			return err
		}
	}

	return c.awaitTx()
}

func (c *initializeContext) isUpdated(ctrHash util.Uint160, cs *contractState) bool {
	realCs, err := c.Client.GetContractStateByHash(ctrHash)
	return err == nil && realCs.NEF.Checksum == cs.NEF.Checksum
}

func (c *initializeContext) getContract(ctrName string) *contractState {
	return c.Contracts[ctrName]
}

func (c *initializeContext) readContracts(names []string) error {
	var (
		fi  os.FileInfo
		err error
	)
	if c.ContractPath != "" {
		fi, err = os.Stat(c.ContractPath)
		if err != nil {
			return fmt.Errorf("invalid contracts path: %w", err)
		}
	}

	if c.ContractPath != "" && fi.IsDir() {
		for _, ctrName := range names {
			cs := new(contractState)
			cs.RawNEF, err = ioutil.ReadFile(path.Join(c.ContractPath, ctrName, ctrName+"_contract.nef"))
			if err != nil {
				return fmt.Errorf("can't read NEF file for %s contract: %w", ctrName, err)
			}
			cs.RawManifest, err = ioutil.ReadFile(path.Join(c.ContractPath, ctrName, "config.json"))
			if err != nil {
				return fmt.Errorf("can't read manifest file for %s contract: %w", ctrName, err)
			}
			c.Contracts[ctrName] = cs
		}
	} else {
		var r io.ReadCloser
		if c.ContractPath == "" {
			c.Command.Println("Contracts flag is missing, latest release will be fetched from Github.")
			r, err = downloadContractsFromGithub(c.Command)
		} else {
			r, err = os.Open(c.ContractPath)
		}
		if err != nil {
			return fmt.Errorf("can't open contracts archive: %w", err)
		}
		defer r.Close()

		m, err := readContractsFromArchive(r, names)
		if err != nil {
			return err
		}
		for _, name := range names {
			c.Contracts[name] = m[name]
		}
	}

	for _, ctrName := range names {
		cs := c.Contracts[ctrName]
		if err := cs.parse(); err != nil {
			return err
		}

		if ctrName != alphabetContract {
			cs.Hash = state.CreateContractHash(c.CommitteeAcc.Contract.ScriptHash(),
				cs.NEF.Checksum, cs.Manifest.Name)
		}
	}
	return nil
}

func (cs *contractState) parse() error {
	nf, err := nef.FileFromBytes(cs.RawNEF)
	if err != nil {
		return fmt.Errorf("can't parse NEF file: %w", err)
	}

	m := new(manifest.Manifest)
	if err := json.Unmarshal(cs.RawManifest, m); err != nil {
		return fmt.Errorf("can't parse manifest file: %w", err)
	}

	cs.NEF = &nf
	cs.Manifest = m
	return nil
}

func readContractsFromArchive(file io.Reader, names []string) (map[string]*contractState, error) {
	m := make(map[string]*contractState, len(names))
	for i := range names {
		m[names[i]] = new(contractState)
	}

	gr, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("contracts file must be tar.gz archive: %w", err)
	}

	r := tar.NewReader(gr)
	for h, err := r.Next(); ; h, err = r.Next() {
		if err != nil {
			break
		}

		dir, _ := path.Split(h.Name)
		ctrName := path.Base(dir)

		cs, ok := m[ctrName]
		if !ok {
			continue
		}

		switch {
		case strings.HasSuffix(h.Name, path.Join(ctrName, ctrName+"_contract.nef")):
			cs.RawNEF, err = ioutil.ReadAll(r)
			if err != nil {
				return nil, fmt.Errorf("can't read NEF file for %s contract: %w", ctrName, err)
			}
		case strings.HasSuffix(h.Name, "config.json"):
			cs.RawManifest, err = ioutil.ReadAll(r)
			if err != nil {
				return nil, fmt.Errorf("can't read manifest file for %s contract: %w", ctrName, err)
			}
		}
		m[ctrName] = cs
	}

	for ctrName, cs := range m {
		if cs.RawNEF == nil {
			return nil, fmt.Errorf("NEF for %s contract wasn't found", ctrName)
		}
		if cs.RawManifest == nil {
			return nil, fmt.Errorf("manifest for %s contract wasn't found", ctrName)
		}
	}
	return m, nil
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
		// In case if NNS is updated multiple times, we can't calculate
		// it's actual hash based on local data, thus query chain.
		nnsCs, err := c.Client.GetContractStateByID(1)
		if err != nil {
			panic("NNS is not yet deployed")
		}
		items = append(items,
			newContractParameter(smartcontract.Hash160Type, c.Contracts[netmapContract].Hash),
			newContractParameter(smartcontract.Hash160Type, c.Contracts[balanceContract].Hash),
			newContractParameter(smartcontract.Hash160Type, c.Contracts[neofsIDContract].Hash),
			newContractParameter(smartcontract.Hash160Type, nnsCs.Hash),
			newContractParameter(smartcontract.StringType, "container"))
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
			{Type: smartcontract.StringType, Value: netmapContainerAliasFeeKey},
			{Type: smartcontract.IntegerType, Value: viper.GetInt64(containerAliasFeeInitFlag)},
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
