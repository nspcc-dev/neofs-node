package morph

import (
	"archive/tar"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	io2 "github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-node/pkg/innerring"
	morphClient "github.com/nspcc-dev/neofs-node/pkg/morph/client"
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
	subnetContract     = "subnet"
)

const (
	netmapEpochKey                   = "EpochDuration"
	netmapMaxObjectSizeKey           = "MaxObjectSize"
	netmapAuditFeeKey                = "AuditFee"
	netmapContainerFeeKey            = "ContainerFee"
	netmapContainerAliasFeeKey       = "ContainerAliasFee"
	netmapEigenTrustIterationsKey    = "EigenTrustIterations"
	netmapEigenTrustAlphaKey         = "EigenTrustAlpha"
	netmapBasicIncomeRateKey         = "BasicIncomeRate"
	netmapInnerRingCandidateFeeKey   = "InnerRingCandidateFee"
	netmapWithdrawFeeKey             = "WithdrawFee"
	netmapHomomorphicHashDisabledKey = "HomomorphicHashingDisabled"

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
		subnetContract,
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

const (
	updateMethodName = "update"
	deployMethodName = "deploy"
)

func (c *initializeContext) deployNNS(method string) error {
	cs := c.getContract(nnsContract)
	h := cs.Hash

	nnsCs, err := c.nnsContractState()
	if err == nil {
		if nnsCs.NEF.Checksum == cs.NEF.Checksum {
			c.Command.Println("NNS contract is already deployed.")
			return nil
		}
		h = nnsCs.Hash
	}

	err = c.addManifestGroup(h, cs)
	if err != nil {
		return fmt.Errorf("can't sign manifest group: %v", err)
	}

	params := getContractDeployParameters(cs, nil)
	signer := transaction.Signer{
		Account: c.CommitteeAcc.Contract.ScriptHash(),
		Scopes:  transaction.CalledByEntry,
	}

	invokeHash := c.nativeHash(nativenames.Management)
	if method == updateMethodName {
		invokeHash = nnsCs.Hash
	}

	res, err := invokeFunction(c.Client, invokeHash, method, params, []transaction.Signer{signer})
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

func (c *initializeContext) updateContracts() error {
	mgmtHash := c.nativeHash(nativenames.Management)
	alphaCs := c.getContract(alphabetContract)

	nnsCs, err := c.nnsContractState()
	if err != nil {
		return err
	}
	nnsHash := nnsCs.Hash

	totalGasCost := int64(0)
	w := io2.NewBufBinWriter()

	var keysParam []interface{}

	// Update script size for a single-node committee is close to the maximum allowed size of 65535.
	// Because of this we want to reuse alphabet contract NEF and manifest for different updates.
	// The generated script is as following.
	// 1. Initialize static slots for alphabet NEF and manifest.
	// 2. Store NEF and manifest into static slots.
	// 3. Push parameters for each alphabet contract on stack.
	// 4. For each alphabet contract, invoke `update` using parameters on stack and
	//    NEF and manifest from step 2.
	// 5. Update other contracts as usual.
	emit.Instruction(w.BinWriter, opcode.INITSSLOT, []byte{2})
	emit.Bytes(w.BinWriter, alphaCs.RawManifest)
	emit.Bytes(w.BinWriter, alphaCs.RawNEF)
	emit.Opcodes(w.BinWriter, opcode.STSFLD0, opcode.STSFLD1)

	// alphabet contracts should be deployed by individual nodes to get different hashes.
	for i, acc := range c.Accounts {
		ctrHash, err := nnsResolveHash(c.Client, nnsHash, getAlphabetNNSDomain(i))
		if err != nil {
			return fmt.Errorf("can't resolve hash for contract update: %w", err)
		}

		keysParam = append(keysParam, acc.PrivateKey().PublicKey().Bytes())

		params := c.getAlphabetDeployItems(i, len(c.Wallets))
		emit.Array(w.BinWriter, params...)
		emit.Opcodes(w.BinWriter, opcode.LDSFLD1, opcode.LDSFLD0)
		emit.Int(w.BinWriter, 3)
		emit.Opcodes(w.BinWriter, opcode.PACK)
		emit.AppCallNoArgs(w.BinWriter, ctrHash, updateMethodName, callflag.All)
	}

	res, err := c.Client.InvokeScript(w.Bytes(), []transaction.Signer{{
		Account: c.CommitteeAcc.Contract.ScriptHash(),
		Scopes:  transaction.Global,
	}})
	if err != nil {
		return fmt.Errorf("can't update alphabet contracts: %w", err)
	}
	if res.State != vm.HaltState.String() {
		return fmt.Errorf("can't update alphabet contracts: %s", res.FaultException)
	}

	w.Reset()
	totalGasCost += res.GasConsumed
	w.WriteBytes(res.Script)

	for _, ctrName := range contractList {
		cs := c.getContract(ctrName)

		method := updateMethodName
		ctrHash, err := nnsResolveHash(c.Client, nnsHash, ctrName+".neofs")
		if err != nil {
			if errors.Is(err, errMissingNNSRecord) {
				// if contract not found we deploy it instead of update
				method = deployMethodName
			} else {
				return fmt.Errorf("can't resolve hash for contract update: %w", err)
			}
		}

		err = c.addManifestGroup(ctrHash, alphaCs)
		if err != nil {
			return fmt.Errorf("can't sign manifest group: %v", err)
		}

		invokeHash := mgmtHash
		if method == updateMethodName {
			invokeHash = ctrHash
		}

		params := getContractDeployParameters(cs, c.getContractDeployData(ctrName, keysParam))
		signer := transaction.Signer{
			Account: c.CommitteeAcc.Contract.ScriptHash(),
			Scopes:  transaction.Global,
		}

		res, err := invokeFunction(c.Client, invokeHash, method, params, []transaction.Signer{signer})
		if err != nil {
			return fmt.Errorf("can't deploy %s contract: %w", ctrName, err)
		}
		if res.State != vm.HaltState.String() {
			return fmt.Errorf("can't deploy %s contract: %s", ctrName, res.FaultException)
		}

		totalGasCost += res.GasConsumed
		w.WriteBytes(res.Script)

		if method == deployMethodName {
			// same actions are done in initializeContext.setNNS, can be unified
			domain := ctrName + ".neofs"
			script, err := c.nnsRegisterDomainScript(nnsHash, cs.Hash, domain)
			if err != nil {
				return err
			}
			if script != nil {
				totalGasCost += defaultRegisterSysfee + native.GASFactor
				w.WriteBytes(script)
			}
			c.Command.Printf("NNS: Set %s -> %s\n", domain, cs.Hash.StringLE())
		}
	}

	groupKey := c.ContractWallet.Accounts[0].PrivateKey().PublicKey()
	sysFee, err := c.emitUpdateNNSGroupScript(w, nnsHash, groupKey)
	if err != nil {
		return err
	}
	c.Command.Printf("NNS: Set %s -> %s\n", morphClient.NNSGroupKeyName, hex.EncodeToString(groupKey.Bytes()))

	totalGasCost += sysFee
	if err := c.sendCommitteeTx(w.Bytes(), totalGasCost, false); err != nil {
		return err
	}
	return c.awaitTx()
}

func (c *initializeContext) deployContracts() error {
	mgmtHash := c.nativeHash(nativenames.Management)
	alphaCs := c.getContract(alphabetContract)

	var keysParam []interface{}

	baseGroups := alphaCs.Manifest.Groups

	// alphabet contracts should be deployed by individual nodes to get different hashes.
	for i, acc := range c.Accounts {
		ctrHash := state.CreateContractHash(acc.Contract.ScriptHash(), alphaCs.NEF.Checksum, alphaCs.Manifest.Name)
		if c.isUpdated(ctrHash, alphaCs) {
			c.Command.Printf("Alphabet contract #%d is already deployed.\n", i)
			continue
		}

		alphaCs.Manifest.Groups = baseGroups
		err := c.addManifestGroup(ctrHash, alphaCs)
		if err != nil {
			return fmt.Errorf("can't sign manifest group: %v", err)
		}

		keysParam = append(keysParam, acc.PrivateKey().PublicKey().Bytes())
		params := getContractDeployParameters(alphaCs, c.getAlphabetDeployItems(i, len(c.Wallets)))

		res, err := invokeFunction(c.Client, mgmtHash, deployMethodName, params, []transaction.Signer{{
			Account: acc.Contract.ScriptHash(),
			Scopes:  transaction.CalledByEntry,
		}})
		if err != nil {
			return fmt.Errorf("can't deploy alphabet #%d contract: %w", i, err)
		}
		if res.State != vm.HaltState.String() {
			return fmt.Errorf("can't deploy alpabet #%d contract: %s", i, res.FaultException)
		}

		if err := c.sendSingleTx(res.Script, res.GasConsumed, acc); err != nil {
			return err
		}
	}

	for _, ctrName := range contractList {
		cs := c.getContract(ctrName)

		ctrHash := cs.Hash
		if c.isUpdated(ctrHash, cs) {
			c.Command.Printf("%s contract is already deployed.\n", ctrName)
			continue
		}

		err := c.addManifestGroup(ctrHash, cs)
		if err != nil {
			return fmt.Errorf("can't sign manifest group: %v", err)
		}

		params := getContractDeployParameters(cs, c.getContractDeployData(ctrName, keysParam))
		signer := transaction.Signer{
			Account: c.CommitteeAcc.Contract.ScriptHash(),
			Scopes:  transaction.Global,
		}

		res, err := invokeFunction(c.Client, mgmtHash, deployMethodName, params, []transaction.Signer{signer})
		if err != nil {
			return fmt.Errorf("can't deploy %s contract: %w", ctrName, err)
		}
		if res.State != vm.HaltState.String() {
			return fmt.Errorf("can't deploy %s contract: %s", ctrName, res.FaultException)
		}

		if err := c.sendCommitteeTx(res.Script, res.GasConsumed, false); err != nil {
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
			cs.RawNEF, err = os.ReadFile(filepath.Join(c.ContractPath, ctrName, ctrName+"_contract.nef"))
			if err != nil {
				return fmt.Errorf("can't read NEF file for %s contract: %w", ctrName, err)
			}
			cs.RawManifest, err = os.ReadFile(filepath.Join(c.ContractPath, ctrName, "config.json"))
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

		dir, _ := filepath.Split(h.Name)
		ctrName := filepath.Base(dir)

		cs, ok := m[ctrName]
		if !ok {
			continue
		}

		switch {
		case strings.HasSuffix(h.Name, filepath.Join(ctrName, ctrName+"_contract.nef")):
			cs.RawNEF, err = io.ReadAll(r)
			if err != nil {
				return nil, fmt.Errorf("can't read NEF file for %s contract: %w", ctrName, err)
			}
		case strings.HasSuffix(h.Name, "config.json"):
			cs.RawManifest, err = io.ReadAll(r)
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

func getContractDeployParameters(cs *contractState, deployData []interface{}) []interface{} {
	return []interface{}{cs.RawNEF, cs.RawManifest, deployData}
}

func (c *initializeContext) getContractDeployData(ctrName string, keysParam []interface{}) []interface{} {
	items := make([]interface{}, 1, 6)
	items[0] = false // notaryDisabled is false

	switch ctrName {
	case neofsContract:
		items = append(items,
			c.Contracts[processingContract].Hash,
			keysParam,
			smartcontract.Parameter{})
	case processingContract:
		items = append(items, c.Contracts[neofsContract].Hash)
		return items[1:] // no notary info
	case auditContract:
		items = append(items, c.Contracts[netmapContract].Hash)
	case balanceContract:
		items = append(items,
			c.Contracts[netmapContract].Hash,
			c.Contracts[containerContract].Hash)
	case containerContract:
		// In case if NNS is updated multiple times, we can't calculate
		// it's actual hash based on local data, thus query chain.
		nnsCs, err := c.Client.GetContractStateByID(1)
		if err != nil {
			panic("NNS is not yet deployed")
		}
		items = append(items,
			c.Contracts[netmapContract].Hash,
			c.Contracts[balanceContract].Hash,
			c.Contracts[neofsIDContract].Hash,
			nnsCs.Hash,
			"container")
	case neofsIDContract:
		items = append(items,
			c.Contracts[netmapContract].Hash,
			c.Contracts[containerContract].Hash)
	case netmapContract:
		configParam := []interface{}{
			netmapEpochKey, viper.GetInt64(epochDurationInitFlag),
			netmapMaxObjectSizeKey, viper.GetInt64(maxObjectSizeInitFlag),
			netmapAuditFeeKey, viper.GetInt64(auditFeeInitFlag),
			netmapContainerFeeKey, viper.GetInt64(containerFeeInitFlag),
			netmapContainerAliasFeeKey, viper.GetInt64(containerAliasFeeInitFlag),
			netmapEigenTrustIterationsKey, int64(defaultEigenTrustIterations),
			netmapEigenTrustAlphaKey, defaultEigenTrustAlpha,
			netmapBasicIncomeRateKey, viper.GetInt64(incomeRateInitFlag),
			netmapInnerRingCandidateFeeKey, viper.GetInt64(candidateFeeInitFlag),
			netmapWithdrawFeeKey, viper.GetInt64(withdrawFeeInitFlag),
			netmapHomomorphicHashDisabledKey, viper.GetBool(homomorphicHashDisabledInitFlag),
		}
		items = append(items,
			c.Contracts[balanceContract].Hash,
			c.Contracts[containerContract].Hash,
			keysParam,
			configParam)
	case proxyContract:
		items = nil
	case reputationContract:
	case subnetContract:
	default:
		panic(fmt.Sprintf("invalid contract name: %s", ctrName))
	}
	return items
}

func (c *initializeContext) getAlphabetDeployItems(i, n int) []interface{} {
	items := make([]interface{}, 6)
	items[0] = false
	items[1] = c.Contracts[netmapContract].Hash
	items[2] = c.Contracts[proxyContract].Hash
	items[3] = innerring.GlagoliticLetter(i).String()
	items[4] = int64(i)
	items[5] = int64(n)
	return items
}
