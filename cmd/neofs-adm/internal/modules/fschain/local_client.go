package fschain

import (
	"crypto/elliptic"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/chaindump"
	"github.com/nspcc-dev/neo-go/pkg/core/fee"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type localClient struct {
	bc           *core.Blockchain
	transactions []*transaction.Transaction
	dumpPath     string
	accounts     []*wallet.Account
	maxGasInvoke int64
}

func newLocalClient(cmd *cobra.Command, v *viper.Viper, wallets []*wallet.Wallet) (*localClient, error) {
	cfg, err := config.LoadFile(v.GetString(protoConfigPath))
	if err != nil {
		return nil, err
	}

	bc, err := core.NewBlockchain(storage.NewMemoryStore(), cfg.Blockchain(), zap.NewNop())
	if err != nil {
		return nil, err
	}

	m := smartcontract.GetDefaultHonestNodeCount(int(cfg.ProtocolConfiguration.ValidatorsCount))
	accounts := make([]*wallet.Account, len(wallets))
	for i := range accounts {
		accounts[i], err = getWalletAccount(wallets[i], consensusAccountName)
		if err != nil {
			return nil, err
		}
	}

	indexMap := make(map[string]int)
	for i, pub := range cfg.ProtocolConfiguration.StandbyCommittee {
		indexMap[pub] = i
	}

	sort.Slice(accounts, func(i, j int) bool {
		pi := accounts[i].PublicKey().Bytes()
		pj := accounts[j].PublicKey().Bytes()
		return indexMap[string(pi)] < indexMap[string(pj)]
	})
	sort.Slice(accounts[:cfg.ProtocolConfiguration.ValidatorsCount], func(i, j int) bool {
		return accounts[i].PublicKey().Cmp(accounts[j].PublicKey()) == -1
	})

	go bc.Run()

	dumpPath := v.GetString(localDumpFlag)
	if cmd.Name() != "init" {
		f, err := os.OpenFile(dumpPath, os.O_RDONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("can't open local dump: %w", err)
		}
		defer f.Close()

		r := io.NewBinReaderFromIO(f)

		var skip uint32
		if bc.BlockHeight() != 0 {
			skip = bc.BlockHeight() + 1
		}

		count := r.ReadU32LE() - skip
		if err := chaindump.Restore(bc, r, skip, count, nil); err != nil {
			return nil, fmt.Errorf("can't restore local dump: %w", err)
		}
	}

	return &localClient{
		bc:           bc,
		dumpPath:     dumpPath,
		accounts:     accounts[:m],
		maxGasInvoke: 15_0000_0000,
	}, nil
}

func (l *localClient) GetBlockCount() (uint32, error) {
	return l.bc.BlockHeight(), nil
}

func (l *localClient) GetContractStateByID(id int32) (*state.Contract, error) {
	h, err := l.bc.GetContractScriptHash(id)
	if err != nil {
		return nil, err
	}
	return l.GetContractStateByHash(h)
}

func (l *localClient) GetContractStateByHash(h util.Uint160) (*state.Contract, error) {
	if cs := l.bc.GetContractState(h); cs != nil {
		return cs, nil
	}
	return nil, storage.ErrKeyNotFound
}

func (l *localClient) GetNativeContracts() ([]state.Contract, error) {
	return l.bc.GetNatives(), nil
}

func (l *localClient) GetApplicationLog(h util.Uint256, t *trigger.Type) (*result.ApplicationLog, error) {
	aer, err := l.bc.GetAppExecResults(h, *t)
	if err != nil {
		return nil, err
	}

	a := result.NewApplicationLog(h, aer, *t)
	return &a, nil
}

func (l *localClient) GetCommittee() (keys.PublicKeys, error) {
	// not used by `morph init` command
	panic("unexpected call")
}

// InvokeFunction is implemented via `InvokeScript`.
func (l *localClient) InvokeFunction(h util.Uint160, method string, sPrm []smartcontract.Parameter, ss []transaction.Signer) (*result.Invoke, error) {
	var err error

	pp := make([]any, len(sPrm))
	for i, p := range sPrm {
		pp[i], err = smartcontract.ExpandParameterToEmitable(p)
		if err != nil {
			return nil, fmt.Errorf("incorrect parameter type %s: %w", p.Type, err)
		}
	}

	return invokeFunction(l, h, method, pp, ss)
}

func (l *localClient) TerminateSession(_ uuid.UUID) (bool, error) {
	// not used by `morph init` command
	panic("unexpected call")
}

func (l *localClient) TraverseIterator(_, _ uuid.UUID, _ int) ([]stackitem.Item, error) {
	// not used by `morph init` command
	panic("unexpected call")
}

// GetVersion return default version.
func (l *localClient) GetVersion() (*result.Version, error) {
	return &result.Version{}, nil
}

func (l *localClient) InvokeContractVerify(_ util.Uint160, _ []smartcontract.Parameter, _ []transaction.Signer, _ ...transaction.Witness) (*result.Invoke, error) {
	// not used by `morph init` command
	panic("unexpected call")
}

// CalculateNetworkFee calculates network fee for the given transaction.
// Copied from neo-go with minor corrections (no need to support non-notary mode):
// https://github.com/nspcc-dev/neo-go/blob/v0.99.2/pkg/services/rpcsrv/server.go#L744
func (l *localClient) CalculateNetworkFee(tx *transaction.Transaction) (int64, error) {
	hashablePart, err := tx.EncodeHashableFields()
	if err != nil {
		return 0, fmt.Errorf("failed to compute tx size: %w", err)
	}

	size := len(hashablePart) + io.GetVarSize(len(tx.Signers))
	ef := l.bc.GetBaseExecFee()

	var netFee int64
	for i, signer := range tx.Signers {
		var verificationScript []byte
		for _, w := range tx.Scripts {
			if w.VerificationScript != nil && hash.Hash160(w.VerificationScript).Equals(signer.Account) {
				verificationScript = w.VerificationScript
				break
			}
		}
		if verificationScript == nil {
			gasConsumed, err := l.bc.VerifyWitness(signer.Account, tx, &tx.Scripts[i], l.maxGasInvoke)
			if err != nil {
				return 0, fmt.Errorf("invalid signature: %w", err)
			}
			netFee += gasConsumed
			size += io.GetVarSize([]byte{}) + io.GetVarSize(tx.Scripts[i].InvocationScript)
			continue
		}

		verificationFee, sizeDelta := fee.Calculate(ef, verificationScript)
		netFee += verificationFee
		size += sizeDelta
	}

	netFee += int64(size) * l.bc.FeePerByte()

	return netFee, nil
}

func (l *localClient) InvokeScript(script []byte, signers []transaction.Signer) (*result.Invoke, error) {
	lastBlock, err := l.bc.GetBlock(l.bc.CurrentBlockHash())
	if err != nil {
		return nil, err
	}

	tx := transaction.New(script, 0)
	tx.Signers = signers
	tx.ValidUntilBlock = l.bc.BlockHeight() + 2

	ic, err := l.bc.GetTestVM(trigger.Application, tx, &block.Block{
		Header: block.Header{
			Index:     lastBlock.Index + 1,
			Timestamp: lastBlock.Timestamp + 1,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("get test VM: %w", err)
	}

	ic.VM.GasLimit = 100_0000_0000
	ic.VM.LoadScriptWithFlags(script, callflag.All)

	var errStr string
	if err := ic.VM.Run(); err != nil {
		errStr = err.Error()
	}
	return &result.Invoke{
		State:          ic.VM.State().String(),
		GasConsumed:    ic.VM.GasConsumed(),
		Script:         script,
		Stack:          ic.VM.Estack().ToArray(),
		FaultException: errStr,
	}, nil
}

func (l *localClient) SendRawTransaction(tx *transaction.Transaction) (util.Uint256, error) {
	// We need to test that transaction was formed correctly to catch as many errors as we can.
	bs := tx.Bytes()
	_, err := transaction.NewTransactionFromBytes(bs)
	if err != nil {
		return tx.Hash(), fmt.Errorf("invalid transaction: %w", err)
	}

	l.transactions = append(l.transactions, tx)
	return tx.Hash(), nil
}

func (l *localClient) putTransactions() error {
	// 1. Prepare new block.
	lastBlock, err := l.bc.GetBlock(l.bc.CurrentBlockHash())
	if err != nil {
		panic(err)
	}
	defer func() { l.transactions = l.transactions[:0] }()

	b := &block.Block{
		Header: block.Header{
			NextConsensus: l.accounts[0].Contract.ScriptHash(),
			Script: transaction.Witness{
				VerificationScript: l.accounts[0].Contract.Script,
			},
			Timestamp: lastBlock.Timestamp + 1,
		},
		Transactions: l.transactions,
	}

	if l.bc.GetConfig().StateRootInHeader {
		b.StateRootEnabled = true
		b.PrevStateRoot = l.bc.GetStateModule().CurrentLocalStateRoot()
	}
	b.PrevHash = lastBlock.Hash()
	b.Index = lastBlock.Index + 1
	b.RebuildMerkleRoot()

	// 2. Sign prepared block.
	var invocationScript []byte

	magic := l.bc.GetConfig().Magic
	for _, acc := range l.accounts {
		sign := acc.SignHashable(magic, b)
		invocationScript = append(invocationScript, byte(opcode.PUSHDATA1), 64)
		invocationScript = append(invocationScript, sign...)
	}
	b.Script.InvocationScript = invocationScript

	// 3. Persist block.
	return l.bc.AddBlock(b)
}

func invokeFunction(c Client, h util.Uint160, method string, parameters []any, signers []transaction.Signer) (*result.Invoke, error) {
	b := smartcontract.NewBuilder()
	b.InvokeMethod(h, method, parameters...)

	script, err := b.Script()
	if err != nil {
		return nil, fmt.Errorf("BUG: invalid parameters for '%s': %w", method, err)
	}

	return c.InvokeScript(script, signers)
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

func (l *localClient) dump() (err error) {
	defer l.bc.Close()

	f, err := os.Create(l.dumpPath)
	if err != nil {
		return err
	}
	defer func() {
		closeErr := f.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	w := io.NewBinWriterFromIO(f)
	w.WriteU32LE(l.bc.BlockHeight() + 1)
	err = chaindump.Dump(l.bc, w, 0, l.bc.BlockHeight()+1)
	return
}
