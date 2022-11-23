package morph

import (
	"crypto/elliptic"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
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
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/io"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/callflag"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/emit"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
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

	bc, err := core.NewBlockchain(storage.NewMemoryStore(), cfg.ProtocolConfiguration, zap.NewNop())
	if err != nil {
		return nil, err
	}

	m := smartcontract.GetDefaultHonestNodeCount(cfg.ProtocolConfiguration.ValidatorsCount)
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
		pi := accounts[i].PrivateKey().PublicKey().Bytes()
		pj := accounts[j].PrivateKey().PublicKey().Bytes()
		return indexMap[string(pi)] < indexMap[string(pj)]
	})
	sort.Slice(accounts[:cfg.ProtocolConfiguration.ValidatorsCount], func(i, j int) bool {
		return accounts[i].PublicKey().Cmp(accounts[j].PublicKey()) == -1
	})

	go bc.Run()

	dumpPath := v.GetString(localDumpFlag)
	if cmd.Name() != "init" {
		f, err := os.OpenFile(dumpPath, os.O_RDONLY, 0600)
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

func (l *localClient) GetNativeContracts() ([]state.NativeContract, error) {
	return l.bc.GetNatives(), nil
}

func (l *localClient) GetNetwork() (netmode.Magic, error) {
	return l.bc.GetConfig().Magic, nil
}

func (l *localClient) GetApplicationLog(h util.Uint256, t *trigger.Type) (*result.ApplicationLog, error) {
	aer, err := l.bc.GetAppExecResults(h, *t)
	if err != nil {
		return nil, err
	}

	a := result.NewApplicationLog(h, aer, *t)
	return &a, nil
}

func (l *localClient) CreateTxFromScript(script []byte, acc *wallet.Account, sysFee int64, netFee int64, cosigners []rpcclient.SignerAccount) (*transaction.Transaction, error) {
	signers, accounts, err := getSigners(acc, cosigners)
	if err != nil {
		return nil, fmt.Errorf("failed to construct tx signers: %w", err)
	}
	if sysFee < 0 {
		res, err := l.InvokeScript(script, signers)
		if err != nil {
			return nil, fmt.Errorf("can't add system fee to transaction: %w", err)
		}
		if res.State != "HALT" {
			return nil, fmt.Errorf("can't add system fee to transaction: bad vm state: %s due to an error: %s", res.State, res.FaultException)
		}
		sysFee = res.GasConsumed
	}

	tx := transaction.New(script, sysFee)
	tx.Signers = signers
	tx.ValidUntilBlock = l.bc.BlockHeight() + 2

	err = l.AddNetworkFee(tx, netFee, accounts...)
	if err != nil {
		return nil, fmt.Errorf("failed to add network fee: %w", err)
	}

	return tx, nil
}

func (l *localClient) GetCommittee() (keys.PublicKeys, error) {
	// not used by `morph init` command
	panic("unexpected call")
}

// InvokeFunction is implemented via `InvokeScript`.
func (l *localClient) InvokeFunction(h util.Uint160, method string, sPrm []smartcontract.Parameter, ss []transaction.Signer) (*result.Invoke, error) {
	var err error

	pp := make([]interface{}, len(sPrm))
	for i, p := range sPrm {
		pp[i], err = smartcontract.ExpandParameterToEmitable(p)
		if err != nil {
			return nil, fmt.Errorf("incorrect parameter type %s: %w", p.Type, err)
		}
	}

	return invokeFunction(l, h, method, pp, ss)
}

func (l *localClient) CalculateNotaryFee(_ uint8) (int64, error) {
	// not used by `morph init` command
	panic("unexpected call")
}

func (l *localClient) SignAndPushP2PNotaryRequest(_ *transaction.Transaction, _ []byte, _ int64, _ int64, _ uint32, _ *wallet.Account) (*payload.P2PNotaryRequest, error) {
	// not used by `morph init` command
	panic("unexpected call")
}

func (l *localClient) SignAndPushInvocationTx(_ []byte, _ *wallet.Account, _ int64, _ fixedn.Fixed8, _ []rpcclient.SignerAccount) (util.Uint256, error) {
	// not used by `morph init` command
	panic("unexpected call")
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

func (l *localClient) InvokeContractVerify(contract util.Uint160, params []smartcontract.Parameter, signers []transaction.Signer, witnesses ...transaction.Witness) (*result.Invoke, error) {
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

		fee, sizeDelta := fee.Calculate(ef, verificationScript)
		netFee += fee
		size += sizeDelta
	}

	fee := l.bc.FeePerByte()
	netFee += int64(size) * fee

	return netFee, nil
}

// AddNetworkFee adds network fee for each witness script and optional extra
// network fee to transaction. `accs` is an array signer's accounts.
// Copied from neo-go with minor corrections (no need to support contract signers):
// https://github.com/nspcc-dev/neo-go/blob/6ff11baa1b9e4c71ef0d1de43b92a8c541ca732c/pkg/rpc/client/rpc.go#L960
func (l *localClient) AddNetworkFee(tx *transaction.Transaction, extraFee int64, accs ...*wallet.Account) error {
	if len(tx.Signers) != len(accs) {
		return errors.New("number of signers must match number of scripts")
	}

	size := io.GetVarSize(tx)
	ef := l.bc.GetBaseExecFee()
	for i := range tx.Signers {
		netFee, sizeDelta := fee.Calculate(ef, accs[i].Contract.Script)
		tx.NetworkFee += netFee
		size += sizeDelta
	}

	tx.NetworkFee += int64(size)*l.bc.FeePerByte() + extraFee
	return nil
}

// getSigners returns an array of transaction signers and corresponding accounts from
// given sender and cosigners. If cosigners list already contains sender, the sender
// will be placed at the start of the list.
// Copied from neo-go with minor corrections:
// https://github.com/nspcc-dev/neo-go/blob/6ff11baa1b9e4c71ef0d1de43b92a8c541ca732c/pkg/rpc/client/rpc.go#L735
func getSigners(sender *wallet.Account, cosigners []rpcclient.SignerAccount) ([]transaction.Signer, []*wallet.Account, error) {
	var (
		signers  []transaction.Signer
		accounts []*wallet.Account
	)

	from := sender.Contract.ScriptHash()
	s := transaction.Signer{
		Account: from,
		Scopes:  transaction.None,
	}
	for _, c := range cosigners {
		if c.Signer.Account == from {
			s = c.Signer
			continue
		}
		signers = append(signers, c.Signer)
		accounts = append(accounts, c.Account)
	}
	signers = append([]transaction.Signer{s}, signers...)
	accounts = append([]*wallet.Account{sender}, accounts...)
	return signers, accounts, nil
}

func (l *localClient) NEP17BalanceOf(h util.Uint160, acc util.Uint160) (int64, error) {
	res, err := invokeFunction(l, h, "balanceOf", []interface{}{acc}, nil)
	if err != nil {
		return 0, err
	}
	if res.State != vmstate.Halt.String() || len(res.Stack) == 0 {
		return 0, fmt.Errorf("`balance`: invalid response (empty: %t): %s",
			len(res.Stack) == 0, res.FaultException)
	}
	bi, err := res.Stack[0].TryInteger()
	if err != nil || !bi.IsInt64() {
		return 0, fmt.Errorf("`balance`: invalid response")
	}
	return bi.Int64(), nil
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
		sign := acc.PrivateKey().SignHashable(uint32(magic), b)
		invocationScript = append(invocationScript, byte(opcode.PUSHDATA1), 64)
		invocationScript = append(invocationScript, sign...)
	}
	b.Script.InvocationScript = invocationScript

	// 3. Persist block.
	return l.bc.AddBlock(b)
}

func invokeFunction(c Client, h util.Uint160, method string, parameters []interface{}, signers []transaction.Signer) (*result.Invoke, error) {
	w := io.NewBufBinWriter()
	emit.Array(w.BinWriter, parameters...)
	emit.AppCallNoArgs(w.BinWriter, h, method, callflag.All)
	if w.Err != nil {
		panic(fmt.Sprintf("BUG: invalid parameters for '%s': %v", method, w.Err))
	}
	return c.InvokeScript(w.Bytes(), signers)
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
