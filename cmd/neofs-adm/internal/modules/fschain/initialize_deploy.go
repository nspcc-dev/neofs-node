package fschain

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	io2 "github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/management"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neofs-contract/common"
	"github.com/nspcc-dev/neofs-contract/rpc/nns"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/spf13/viper"
)

const (
	nnsContract        = "nns"
	neofsContract      = "neofs"      // not deployed in FS chain.
	processingContract = "processing" // not deployed in FS chain.
	alphabetContract   = "alphabet"
	balanceContract    = "balance"
	containerContract  = "container"
	netmapContract     = "netmap"
	proxyContract      = "proxy"
	reputationContract = "reputation"
)

const (
	netmapEpochKey                   = "EpochDuration"
	netmapMaxObjectSizeKey           = "MaxObjectSize"
	netmapContainerFeeKey            = "ContainerFee"
	netmapContainerAliasFeeKey       = "ContainerAliasFee"
	netmapEigenTrustIterationsKey    = "EigenTrustIterations"
	netmapEigenTrustAlphaKey         = "EigenTrustAlpha"
	netmapBasicIncomeRateKey         = "BasicIncomeRate"
	netmapWithdrawFeeKey             = "WithdrawFee"
	netmapHomomorphicHashDisabledKey = "HomomorphicHashingDisabled"

	defaultEigenTrustIterations = 4
	defaultEigenTrustAlpha      = "0.1"
)

var (
	contractList = []string{
		netmapContract,
		balanceContract,
		containerContract,
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

const (
	updateMethodName = "update"
	deployMethodName = "deploy"
)

func (c *initializeContext) deployNNS(method string) error {
	cs := c.getContract(nnsContract)

	nnsCs, err := c.Client.GetContractStateByID(nns.ID)
	if err == nil {
		if nnsCs.NEF.Checksum == cs.NEF.Checksum {
			if method == deployMethodName {
				c.Command.Println("NNS contract is already deployed.")
			} else {
				c.Command.Println("NNS contract is already updated.")
			}
			return nil
		}
	}
	act, err := actor.NewSimple(c.Client, c.CommitteeAcc)
	if err != nil {
		return fmt.Errorf("creating actor: %w", err)
	}

	var tx *transaction.Transaction
	if method == updateMethodName {
		var nnsCnt = nns.New(act, nnsCs.Hash)
		tx, err = nnsCnt.UpdateUnsigned(cs.RawNEF, cs.RawManifest, nil)
	} else {
		var mgmt = management.New(act)
		tx, err = mgmt.DeployUnsigned(cs.NEF, cs.Manifest, []any{
			[]any{
				[]any{"neofs", "ops@nspcc.io"},
			},
		})
	}
	if err != nil {
		return fmt.Errorf("creating tx: %w", err)
	}

	if err := c.multiSignAndSend(tx, committeeAccountName); err != nil {
		return fmt.Errorf("can't send deploy transaction: %w", err)
	}

	return c.awaitTx()
}

func (c *initializeContext) updateContracts() error {
	alphaCs := c.getContract(alphabetContract)

	nnsHash, nnsReader, err := c.nnsReader()
	if err != nil {
		return err
	}

	w := io2.NewBufBinWriter()

	var keysParam []any

	// Update script size for a single-node committee is close to the maximum allowed size of 65535.
	// Because of this we want to reuse alphabet contract NEF and manifest for different updates.
	// The generated script is as following.
	// 1. Initialize static slot for alphabet NEF.
	// 2. Store NEF into the static slot.
	// 3. Push parameters for each alphabet contract on stack.
	// 5. For each alphabet contract, invoke `update` using parameters on stack and
	//    NEF from step 2 and manifest from step 4.
	emit.Instruction(w.BinWriter, opcode.INITSSLOT, []byte{1})
	emit.Bytes(w.BinWriter, alphaCs.RawNEF)
	emit.Opcodes(w.BinWriter, opcode.STSFLD0)

	// alphabet contracts should be deployed by individual nodes to get different hashes.
	for i, acc := range c.Accounts {
		ctrHash, err := nnsReader.ResolveFSContract(getAlphabetNNSDomain(i))
		if err != nil {
			return fmt.Errorf("can't resolve hash for contract update: %w", err)
		}

		keysParam = append(keysParam, acc.PublicKey().Bytes())

		params := c.getAlphabetDeployItems(i, len(c.Wallets))
		emit.Array(w.BinWriter, params...)
		emit.Bytes(w.BinWriter, alphaCs.RawManifest)
		emit.Opcodes(w.BinWriter, opcode.LDSFLD0)
		emit.Int(w.BinWriter, 3)
		emit.Opcodes(w.BinWriter, opcode.PACK)
		emit.AppCallNoArgs(w.BinWriter, ctrHash, updateMethodName, callflag.All)
	}

	if err := c.sendCommitteeTx(w.Bytes(), false); err != nil {
		if !strings.Contains(err.Error(), common.ErrAlreadyUpdated) {
			return err
		}
		c.Command.Println("Alphabet contracts are already updated.")
	}

	w.Reset()

	emit.Instruction(w.BinWriter, opcode.INITSSLOT, []byte{1})
	emit.AppCall(w.BinWriter, nnsHash, "getPrice", callflag.All)
	emit.Opcodes(w.BinWriter, opcode.STSFLD0)
	emit.AppCall(w.BinWriter, nnsHash, "setPrice", callflag.All, 1)

	for _, ctrName := range contractList {
		cs := c.getContract(ctrName)

		method := updateMethodName
		ctrHash, err := nnsReader.ResolveFSContract(ctrName)
		if err != nil {
			return fmt.Errorf("can't resolve hash for contract update: %w", err)
		}

		invokeHash := management.Hash
		if method == updateMethodName {
			invokeHash = ctrHash
		}

		params := getContractDeployParameters(cs, c.getContractDeployData(ctrHash, ctrName, keysParam))
		res, err := c.CommitteeAct.MakeCall(invokeHash, method, params...)
		if err != nil {
			if method != updateMethodName || !strings.Contains(err.Error(), common.ErrAlreadyUpdated) {
				return fmt.Errorf("deploy contract: %w", err)
			}
			c.Command.Printf("%s contract is already updated.\n", ctrName)
			continue
		}

		w.WriteBytes(res.Script)

		if method == deployMethodName {
			// same actions are done in initializeContext.setNNS, can be unified
			domain := ctrName + ".neofs"
			script, ok, err := c.nnsRegisterDomainScript(nnsHash, cs.Hash, domain)
			if err != nil {
				return err
			}
			if !ok {
				w.WriteBytes(script)
				emit.AppCall(w.BinWriter, nnsHash, "deleteRecords", callflag.All, domain, nns.TXT)
				emit.AppCall(w.BinWriter, nnsHash, "addRecord", callflag.All,
					domain, nns.TXT, cs.Hash.StringLE())
				emit.AppCall(w.BinWriter, nnsHash, "addRecord", callflag.All,
					domain, nns.TXT, address.Uint160ToString(cs.Hash))
			}
			c.Command.Printf("NNS: Set %s -> %s\n", domain, cs.Hash.StringLE())
		}
	}

	emit.Opcodes(w.BinWriter, opcode.LDSFLD0)
	emit.Int(w.BinWriter, 1)
	emit.Opcodes(w.BinWriter, opcode.PACK)
	emit.AppCallNoArgs(w.BinWriter, nnsHash, "setPrice", callflag.All)

	if err := c.sendCommitteeTx(w.Bytes(), true); err != nil {
		return err
	}
	return c.awaitTx()
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
			cs, err := readContract(filepath.Join(c.ContractPath, ctrName), ctrName)
			if err != nil {
				return err
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
			if err := m[name].parse(); err != nil {
				return err
			}
			c.Contracts[name] = m[name]
		}
	}

	for _, ctrName := range names {
		if ctrName != alphabetContract {
			cs := c.Contracts[ctrName]
			cs.Hash = state.CreateContractHash(c.CommitteeAcc.Contract.ScriptHash(),
				cs.NEF.Checksum, cs.Manifest.Name)
		}
	}
	return nil
}

func readContract(ctrPath, ctrName string) (*contractState, error) {
	rawNef, err := os.ReadFile(filepath.Join(ctrPath, ctrName+"_contract.nef"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("can't read NEF file for %s contract: %w", ctrName, err)
		}
		rawNef, err = os.ReadFile(filepath.Join(ctrPath, "contract.nef"))
		if err != nil {
			return nil, fmt.Errorf("can't read NEF file for %s contract: %w", ctrName, err)
		}
	}
	rawManif, err := os.ReadFile(filepath.Join(ctrPath, "config.json"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("can't read manifest file for %s contract: %w", ctrName, err)
		}
		rawManif, err = os.ReadFile(filepath.Join(ctrPath, "manifest.json"))
		if err != nil {
			return nil, fmt.Errorf("can't read manifest file for %s contract: %w", ctrName, err)
		}
	}

	cs := &contractState{
		RawNEF:      rawNef,
		RawManifest: rawManif,
	}

	return cs, cs.parse()
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
		case strings.HasSuffix(h.Name, "contract.nef"):
			cs.RawNEF, err = io.ReadAll(r)
			if err != nil {
				return nil, fmt.Errorf("can't read NEF file for %s contract: %w", ctrName, err)
			}
		case strings.HasSuffix(h.Name, "config.json") ||
			strings.HasSuffix(h.Name, "manifest.json"):
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

func getContractDeployParameters(cs *contractState, deployData []any) []any {
	return []any{cs.RawNEF, cs.RawManifest, deployData}
}

func (c *initializeContext) getContractDeployData(ctrHash util.Uint160, ctrName string, keysParam []any) []any {
	items := make([]any, 1, 6)
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
	case balanceContract:
		items = append(items,
			c.Contracts[netmapContract].Hash,
			c.Contracts[containerContract].Hash)
	case containerContract:
		// In case if NNS is updated multiple times, we can't calculate
		// it's actual hash based on local data, thus query chain.
		nnsHash, err := nns.InferHash(c.Client)
		if err != nil {
			panic("NNS is not yet deployed")
		}
		items = append(items,
			c.Contracts[netmapContract].Hash,
			c.Contracts[balanceContract].Hash,
			util.Uint160{}, // no longer used (https://github.com/nspcc-dev/neofs-contract/issues/474)
			nnsHash,
			"container")
	case netmapContract:
		configParam := []any{}
		for _, kf := range []struct {
			key  string
			flag string
		}{
			{netmapEpochKey, epochDurationInitFlag},
			{netmapMaxObjectSizeKey, maxObjectSizeInitFlag},
			{netmapContainerFeeKey, containerFeeInitFlag},
			{netmapContainerAliasFeeKey, containerAliasFeeInitFlag},
			{netmapBasicIncomeRateKey, incomeRateInitFlag},
			{netmapWithdrawFeeKey, withdrawFeeInitFlag},
		} {
			i64, err := unwrap.Int64(c.ReadOnlyInvoker.Call(ctrHash, "config", kf.key))
			if err != nil {
				i64 = viper.GetInt64(kf.flag)
			}
			configParam = append(configParam, kf.key, i64)
		}
		for _, kf := range []struct {
			key  string
			flag string
		}{
			{netmapHomomorphicHashDisabledKey, homomorphicHashDisabledInitFlag},
		} {
			bval, err := unwrap.Bool(c.ReadOnlyInvoker.Call(ctrHash, "config", kf.key))
			if err != nil {
				bval = viper.GetBool(kf.flag)
			}
			configParam = append(configParam, kf.key, bval)
		}
		configParam = append(configParam, netmapEigenTrustIterationsKey, int64(defaultEigenTrustIterations),
			netmapEigenTrustAlphaKey, defaultEigenTrustAlpha,
		)

		items = append(items,
			c.Contracts[balanceContract].Hash,
			c.Contracts[containerContract].Hash,
			keysParam,
			configParam)
	case proxyContract:
		items = nil
	case reputationContract:
	default:
		panic(fmt.Sprintf("invalid contract name: %s", ctrName))
	}
	return items
}

func (c *initializeContext) getAlphabetDeployItems(i, n int) []any {
	items := make([]any, 6)
	items[0] = false
	items[1] = c.Contracts[netmapContract].Hash
	items[2] = c.Contracts[proxyContract].Hash
	items[3] = client.NNSAlphabetContractName(i)
	items[4] = int64(i)
	items[5] = int64(n)
	return items
}
