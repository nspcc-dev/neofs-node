package blockchain

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/consensus"
	"github.com/nspcc-dev/neo-go/pkg/core"
	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	corestateroot "github.com/nspcc-dev/neo-go/pkg/core/stateroot"
	"github.com/nspcc-dev/neo-go/pkg/core/storage"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/network"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	notarysvc "github.com/nspcc-dev/neo-go/pkg/services/notary"
	"github.com/nspcc-dev/neo-go/pkg/services/rpcsrv"
	"github.com/nspcc-dev/neo-go/pkg/services/stateroot"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-contract/nns"
	"go.uber.org/zap"
)

// Blockchain provides Neo blockchain services consumed by NeoFS Inner Ring. By
// design, Blockchain does not implement Inner Ring specifics: instead, it
// provides the generic functionality of the Neo blockchain, and narrows the
// rich Neo functionality to the minimum necessary for the operation of the
// Inner Ring.
//
// Blockchain must be initialized using New constructor. After initialization
// Blockchain becomes a single-use component that can be started and then
// stopped. All operations should be executed after Blockchain is started and
// before it is stopped (reverse behavior is undefined).
type Blockchain struct {
	base *core.Blockchain

	netServer *network.Server

	nep17GAS *nep17.Token

	actor *actor.Actor

	rpcActor notary.RPCActor

	chErr chan error
}

// SignTools groups signing components for New constructor.
type SignTools struct {
	// NEO account of the node.
	Account *wallet.Account

	// File containing Account.
	AccountFile struct {
		// Path to the file.
		Path string
		// Account password.
		Password string
	}
}

// New returns new Blockchain instance initialized using provided parameters. If
// New returns an error, Blockchain can (and must) not be used with provided
// configuration.
//
// Provided file path is expected to lead to the Neo Go partial (*)
// configuration file: it is used to build core blockchain components similar to
// the Neo Go node in order to use Neo blockchain services.
// (*) partial means that some values are static and can't be configured:
//   - ApplicationConfiguration.Relay is always set
//   - ApplicationConfiguration.X.UnlockWallet is set to specified SignTools.AccountFile
//     (X - Consensus, P2PNotary)
//   - ApplicationConfiguration.X.Enabled is set if node belongs to
//     the committee (X - Consensus, P2PNotary)
//
// Given SignTools.Account is expected to be Inner Ring node's NEO account. It
// must be previously unlocked for signing, and SignTools.AccountFile must
// correspond to it. If the account belongs to the committee of the configured
// Neo blockchain, resulting Blockchain is run in consensus mode.
//
// Given zap.Logger instance is used to log Blockchain events. It should be
// previously initialized: Blockchain does not modify the logger.
//
// Specified error channel is used to report internal Blockchain errors to the
// superior context (Inner Ring application) listening to it in the background.
func New(neoNodeConfigFilepath string, signTools SignTools, log *zap.Logger, chErr chan<- error) (*Blockchain, error) {
	switch {
	case neoNodeConfigFilepath == "":
		panic("missing Neo node config file")
	case signTools.Account == nil:
		panic("missing node account")
	case signTools.Account != nil && !signTools.Account.CanSign():
		panic("account can not sign anything")
	case signTools.AccountFile.Path == "":
		panic("missing path to file with the node account")
	case log == nil:
		panic("missing logger")
	case chErr == nil:
		panic("missing external error channel")
	}

	// TODO: (almost) all code below is copy-pasted from neo-go server command, consider sharing

	cfg, err := config.LoadFile(neoNodeConfigFilepath)
	if err != nil {
		return nil, fmt.Errorf("load neo-go node config from file: %w", err)
	}

	// set static configurations which default to non-zero fields of config.Config
	// structure.
	cfg.ApplicationConfiguration.Relay = true
	// TODO: what else?

	cfgServer, err := network.NewServerConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("compose neo-go server config from the base one: %w", err)
	}

	blockchainStorage, err := storage.NewStore(cfg.ApplicationConfiguration.DBConfiguration)
	if err != nil {
		return nil, fmt.Errorf("init blockchain storage: %w", err)
	}

	blockChain, err := core.NewBlockchain(blockchainStorage, cfg.Blockchain(), log)
	if err != nil {
		closeErr := blockchainStorage.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("init core blockchain component: %w; failed to close blockchain storage: %v", err, closeErr)
		}

		return nil, fmt.Errorf("init core blockchain component: %w", err)
	}

	go blockChain.Run() // is it required to be insta called?

	committee, err := blockChain.GetCommittee()
	if err != nil {
		// currently never returns an error, but this is not documented, so exit would
		// be incorrect.
		return nil, fmt.Errorf("get committee members: %w", err)
	}

	isAlphabetNode := false

	for i := range committee {
		// note that CanSign is checked above
		if isAlphabetNode = committee[i].Equal(signTools.Account.PublicKey()); isAlphabetNode {
			break
		}
	}

	netServer, err := network.NewServer(cfgServer, blockChain, blockChain.GetStateSyncModule(), log)
	if err != nil {
		return nil, fmt.Errorf("init neo-go network server: %w", err)
	}

	stateRootMode := blockChain.GetStateModule().(*corestateroot.Module)

	stateRootService, err := stateroot.New(cfgServer.StateRootCfg, stateRootMode, log, blockChain, netServer.BroadcastExtensible)
	if err != nil {
		return nil, fmt.Errorf("init StateRoot service: %w", err)
	}

	netServer.AddExtensibleService(stateRootService, stateroot.Category, stateRootService.OnPayload)

	// do we need oracle service?

	// var oracleService *oracle.Oracle
	//
	// if cfg.ApplicationConfiguration.Oracle.Enabled {
	// 	var c oracle.Config
	// 	c.Log = log
	// 	c.Network = cfg.ProtocolConfiguration.Magic
	// 	c.MainCfg = cfg.ApplicationConfiguration.Oracle
	// 	c.Chain = artifacts.blockchain
	// 	c.OnTransaction = artifacts.netServer.RelayTxn
	//
	// 	oracleService, err = oracle.NewOracle(c)
	// 	if err != nil {
	// 		return fmt.Errorf("init Oracle module: %w", err)
	// 	}
	//
	// 	artifacts.blockchain.SetOracle(oracleService)
	// 	artifacts.netServer.AddService(oracleService)
	// }

	if isAlphabetNode {
		// currently Consensus and Notary services doesn't support external
		// wallet.Account, so just pass file with the wallet including
		// signTools.Account. Password should work since the account has been already
		// unlocked with it.
		cfgUnlockWallet := config.Wallet{
			Path:     signTools.AccountFile.Path,
			Password: signTools.AccountFile.Password,
		}

		var cfgConsensus consensus.Config
		cfgConsensus.Logger = log
		cfgConsensus.Broadcast = netServer.BroadcastExtensible
		cfgConsensus.Chain = blockChain
		cfgConsensus.ProtocolConfiguration = blockChain.GetConfig().ProtocolConfiguration
		cfgConsensus.RequestTx = netServer.RequestTx
		cfgConsensus.StopTxFlow = netServer.StopTxFlow
		// currently consensus.Service doesn't support external wallet.Account, so just
		// pass file with the wallet including signTools.Account. Password should work
		// since the account has been already unlocked with it.
		cfgConsensus.Wallet = cfgUnlockWallet
		cfgConsensus.TimePerBlock = cfgServer.TimePerBlock

		consensusService, err := consensus.NewService(cfgConsensus)
		if err != nil {
			return nil, fmt.Errorf("init Consensus module: %w", err)
		}

		netServer.AddConsensusService(consensusService, consensusService.OnPayload, consensusService.OnTransaction)

		var cfgNotary notarysvc.Config
		cfgNotary.MainCfg = config.P2PNotary{
			Enabled:      true,
			UnlockWallet: cfgUnlockWallet,
		}
		cfgNotary.Chain = blockChain
		cfgNotary.Log = log

		notaryService, err := notarysvc.NewNotary(cfgNotary, netServer.Net, netServer.GetNotaryPool(), func(tx *transaction.Transaction) error {
			err := netServer.RelayTxn(tx)
			if err != nil && !errors.Is(err, core.ErrAlreadyExists) {
				return fmt.Errorf("relay completed notary transaction %s: %w", tx.Hash().StringLE(), err)
			}

			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("init Notary service: %w", err)
		}

		netServer.AddService(notaryService)
		blockChain.SetNotary(notaryService)
	}

	// "make" channel rw to satisfy Start method
	chErrRw := make(chan error)

	go func() {
		err, ok := <-chErrRw
		if ok {
			chErr <- err
		}
	}()

	rpcServer := rpcsrv.New(blockChain, cfg.ApplicationConfiguration.RPC, netServer, nil, log, chErrRw)

	netServer.AddService(&rpcServer) // is it a cyclic dependency?

	rpcActor, err := rpcclient.NewInternal(context.Background(), rpcServer.RegisterLocal)
	if err != nil {
		return nil, fmt.Errorf("construct internal RPC client: %w", err)
	} else if err = rpcActor.Init(); err != nil {
		return nil, fmt.Errorf("init internal RPC client: %w", err)
	}

	_actor, err := actor.NewSimple(rpcActor, signTools.Account)
	if err != nil {
		return nil, fmt.Errorf("init simple actor using node account: %w", err)
	}

	res := &Blockchain{
		base:      blockChain,
		netServer: netServer,
		nep17GAS:  nep17.New(_actor, gas.Hash),
		actor:     _actor,
		rpcActor:  rpcActor,
		chErr:     chErrRw,
	}

	return res, nil
}

// Run runs the Blockchain and makes all its functionality available for use.
// Returns any error encountered which prevented the Blockchain to be started.
// If Run failed, the Blockchain should no longer be used. After Blockchain has
// been successfully run, all internal failures are written to the channel
// provided to New.
//
// Run should not be called more than once.
//
// Use Stop to stop the Blockchain.
func (x *Blockchain) Run() error {
	go x.netServer.Start(x.chErr)
	return nil
}

// Stop stops the running Blockchain and frees all its internal resources.
//
// Stop should not be called twice and before successful Run.
func (x *Blockchain) Stop() {
	if c, ok := x.rpcActor.(interface{ Close() }); ok {
		c.Close()
	}

	x.netServer.Shutdown()
	x.base.Close()
}

// errContractNotFound is returned when contract from the particular context is
// unavailable.
var errContractNotFound = errors.New("contract not found")

// firstDeployedContract returns first contract deployed in the related
// blockchain. The first contract has identifier #1 in terms of Neo blockchain.
// Returns errContractNotFound if no contracts have been deployed yet.
func (x *Blockchain) firstDeployedContract() (util.Uint160, error) {
	res, err := x.base.GetContractScriptHash(1)
	if errors.Is(err, storage.ErrKeyNotFound) {
		// 'not found' error is not documented in GetContractScriptHash and got from the
		// implementation. If it will be changed, we'll adopt and won't change current
		// method. But this requires precise attention to incoming changes.
		return res, errContractNotFound
	}

	return res, err
}

// contractByName resolve address of the contract by its name registered in the
// NeoFS NNS contract referenced by the given address. Returns
// errContractNotFound if provided name is not registered.
func (x *Blockchain) contractByName(nnsContract util.Uint160, name string) (res util.Uint160, err error) {
	const method = "resolve"

	r, err := x.TestCallContractMethod(nnsContract, "resolve", name, int64(nns.TXT))
	if err != nil {
		if faultExceptionContains(err, "token not found") {
			return res, errContractNotFound
		}

		return res, fmt.Errorf("call NNS contract's '%s' method: %w", method, err)
	}

	item := r.Ret

	// copy-paste from morph lib
	if arr, ok := item.Value().([]stackitem.Item); ok {
		if len(arr) == 0 {
			return res, errors.New("empty NNS record in the response")
		}

		item = arr[0]
	}

	bs, err := item.TryBytes()
	if err != nil {
		return res, fmt.Errorf("malformed NNS record in the response (expected 1st element to be byte array): %w", err)
	}

	res, err = util.Uint160DecodeStringLE(string(bs))
	if err != nil {
		res, err = address.StringToUint160(string(bs))
	}
	if err != nil {
		return res, fmt.Errorf("malformed NNS record in the response (expected 1st element to be 20-byte contract address): %w", err)
	}

	return
}

// WaitForContracts waits until all contracts with the specified names are
// deployed in the related blockchain. When WaitForContracts encounters any of
// the specified contracts in the blockchain, it passes its address to the
// provided handler. WaitForContracts is aborted by the specified channel. If
// WaitForContracts failed, caller should not rely on the presence of the
// awaiting contracts (but the opposite is not guaranteed).
func (x *Blockchain) WaitForContracts(chAbort <-chan struct{}, awaitingContracts []string, handler func(name string, addr util.Uint160)) error {
	var nnsContract util.Uint160
	var err error

	for {
		select {
		case <-chAbort:
			return context.DeadlineExceeded
		default:
		}

		// it's assumed that NNS contract will always have ID=1, is it documented somewhere?
		// if not it would be nice to do so, otherwise assumption is self-willed
		nnsContract, err = x.firstDeployedContract()
		if err == nil {
			break
		} else if err != errContractNotFound {
			return fmt.Errorf("get NNS contract address: %w", err)
		}

		// TODO: configure or reconsider the value
		time.Sleep(3 * time.Second)
	}

	for {
		select {
		case <-chAbort:
			return context.DeadlineExceeded
		default:
		}

		for i := 0; i < len(awaitingContracts); i++ { // don't use range: slice is mutated in the body
			contract, err := x.contractByName(nnsContract, awaitingContracts[i])
			if err != nil {
				if err == errContractNotFound {
					continue
				}

				return fmt.Errorf("resolve '%s' contract address by name using NNS contract: %w", awaitingContracts[i], err)
			}

			handler(awaitingContracts[i], contract)

			if len(awaitingContracts) == 1 { // that was the last contract
				return nil
			}

			awaitingContracts = append(awaitingContracts[:i], awaitingContracts[i+1:]...)
			i--
		}

		// TODO: configure or reconsider the value
		time.Sleep(3 * time.Second)
	}
}

// CallContractMethod creates transaction of Neo smart contract method call and
// spreads it in the blockchain network. Contract is referenced by the given
// address, and particular call is done according to the specified method name
// and various arguments.
func (x *Blockchain) CallContractMethod(contract util.Uint160, method string, args ...interface{}) error {
	tx, err := x.actor.MakeCall(contract, method, args...)
	if err != nil {
		return fmt.Errorf("make call using actor: %w", err)
	}

	err = x.netServer.RelayTxn(tx)
	if err != nil {
		return fmt.Errorf("relay contract call transaction to the network: %w", err)
	}

	return nil
}

// faultException provides built-in error from fault exception thrown by some
// Neo smart contract method call.
type faultException string

func (x faultException) Error() string {
	return fmt.Sprintf("contract call fault exception: %s", string(x))
}

// wrapFaultException wraps given string into faultException error.
func wrapFaultException(e string) error {
	return faultException(e)
}

// faultExceptionContains checks if given error is a faultException which
// contains specified substring.
func faultExceptionContains(err error, substr string) bool {
	var f faultException

	if !errors.As(err, &f) {
		return false
	}

	return strings.Contains(string(f), substr)
}

// TestCallResult groups resulting items of Neo smart contract method call done using
// Blockchain.
type TestCallResult struct {
	// Method's return value wrapped into stackitem.Item. Includes only first return value.
	Ret stackitem.Item
	// Amount of Neo GAS spent on a method call.
	ConsumedGAS int64
	// Encoded call script.
	Script []byte
}

// TestCallContractMethod perform test call of the specified method of the
// contract referenced by the given address with the provided arguments and
// returns resulting artifacts as TestCallResult. Test means that call doesn't
// produce transaction which affects the blockchain.
//
// Returns an error if final state of the used Neo virtual machine is not HALT.
// Returns an error if stack of the resulting values is empty.
func (x *Blockchain) TestCallContractMethod(contract util.Uint160, method string, args ...interface{}) (res TestCallResult, err error) {
	r, err := x.actor.Call(contract, method, args...)
	if err != nil {
		return res, fmt.Errorf("actor call: %w", err)
	}

	if r.State != vmstate.Halt.String() {
		return res, wrapFaultException(r.FaultException)
	} else if len(r.Stack) == 0 {
		return res, errors.New("empty result stack")
	}

	res.Ret = r.Stack[0]
	res.Script = r.Script
	res.ConsumedGAS = r.GasConsumed

	return
}

// GetContractMethodReturn is a wrapper over Blockchain.TestCallContractMethod
// which simplifies access to return value of the smart contract method call return.
func GetContractMethodReturn(b *Blockchain, contract util.Uint160, method string, args ...interface{}) (stackitem.Item, error) {
	res, err := b.TestCallContractMethod(contract, method, args...)
	if err != nil {
		return nil, err
	}

	return res.Ret, nil
}

// Committee returns list of public keys of the Neo committee members.
func (x *Blockchain) Committee() (keys.PublicKeys, error) {
	return x.base.GetCommittee()
}

// TransferGAS transfers specified amount of GAS between given accounts by
// creating a transaction that calls 'transfer' method of the Neo native GAS
// contract and spreading it in the blockchain network.
func (x *Blockchain) TransferGAS(amount uint64, from, to util.Uint160) error {
	_, _, err := x.nep17GAS.Transfer(from, to, new(big.Int).SetUint64(amount), nil)
	if err != nil {
		return fmt.Errorf("transfer GAS using NEP-17 native GAS contract: %w", err)
	}

	return nil
}

// InvocationScriptOfTransactionSignature returns encoded invocation script of signing
// the given transaction on behalf of the specified Neo account.
func (x *Blockchain) InvocationScriptOfTransactionSignature(acc *wallet.Account, tx transaction.Transaction) []byte {
	return append(
		[]byte{byte(opcode.PUSHDATA1), 64},
		acc.PrivateKey().SignHashable(uint32(x.base.GetConfig().Magic), &tx)...,
	)
}

// TransactionBlock returns sequence number of the block with the transaction
// referenced by the given identifier.
func (x *Blockchain) TransactionBlock(txID util.Uint256) (uint32, error) {
	_, height, err := x.base.GetTransaction(txID)
	if err != nil || height == math.MaxUint32 {
		return 0, fmt.Errorf("unknown transaction")
	}

	return height, nil
}

// formMultiSigAccountFromKeys converts given base wallet.Account into
// multi-signature account with number of sufficient signatures calculated
// according to Neo consensus mechanism. Public keys of signers are passed in
// corresponding parameter.
func formMultiSigAccountFromKeys(base *wallet.Account, keys keys.PublicKeys) (*wallet.Account, error) {
	multiSigAcc := wallet.NewAccountFromPrivateKey(base.PrivateKey())

	err := multiSigAcc.ConvertMultisig(smartcontract.GetDefaultHonestNodeCount(len(keys)), keys)
	if err != nil {
		return nil, fmt.Errorf("convert simple account to the multisig one: %w", err)
	}

	return multiSigAcc, nil
}

// returns actor.SignerAccount for the given wallet.Account with
// transaction.WitnessScope set to transaction.CalledByEntry.
func accountToSignerAccount(acc *wallet.Account) (res actor.SignerAccount) {
	res.Account = acc
	res.Signer.Account = acc.ScriptHash()
	res.Signer.Scopes = transaction.CalledByEntry
	return
}

// returns actor.SignerAccount for the contract account which is provided to
// notary.Actor to lay the signature expected by the given contract.
func contractSignerAccount(contract util.Uint160) (res actor.SignerAccount) {
	res.Account = notary.FakeContractAccount(contract)
	res.Signer.Account = contract
	res.Signer.Scopes = transaction.None
	return
}

// SignAndSendTransactionNotary signs given transaction on behalf of the
// specified simple Neo account, wraps the transaction into request for signing
// by group of parties presented in a specified keys.PublicKeys, and spreads it
// in the blockchain network via Neo Notary service.
//
// Passed keys.PublicKeys must not be empty.
func (x *Blockchain) SignAndSendTransactionNotary(tx transaction.Transaction, signerKeys keys.PublicKeys, simpleAcc *wallet.Account) error {
	if len(signerKeys) == 0 {
		panic("missing signer keys")
	}

	multiSigAcc, err := formMultiSigAccountFromKeys(simpleAcc, signerKeys)
	if err != nil {
		return err
	}

	err = multiSigAcc.SignTx(x.actor.GetNetwork(), &tx)
	if err != nil {
		return fmt.Errorf("sign main transaction of the notary request: %w", err)
	}

	return x.submitTransactionNotary(simpleAcc, []actor.SignerAccount{accountToSignerAccount(multiSigAcc)}, func(*notary.Actor) (*transaction.Transaction, error) {
		return &tx, nil
	})
}

// CallContractNotarized creates transaction for calling specified contract
// (particular method with its arguments), wraps the transaction into request
// for signing by:
//  1. Proxy contract deployed in the NeoFS Sidechain
//  2. a group of parties presented in a provided keys.PublicKeys
//  3. Notary native contract
//
// and spreads it in the blockchain network. The action is performed via Neo
// Notary request. The request is signed on behalf of the specified simple Neo
// account.
//
// Passed keys.PublicKeys must not be empty.
func (x *Blockchain) CallContractNotarized(simpleAcc *wallet.Account, signerKeys keys.PublicKeys, proxyContract, calledContract util.Uint160, method string, args ...interface{}) error {
	if len(signerKeys) == 0 {
		panic("missing signer keys")
	}

	multiSigAcc, err := formMultiSigAccountFromKeys(simpleAcc, signerKeys)
	if err != nil {
		return err
	}

	signers := []actor.SignerAccount{
		contractSignerAccount(proxyContract),
		accountToSignerAccount(multiSigAcc),
		contractSignerAccount(notary.Hash),
	}

	return x.submitTransactionNotary(simpleAcc, signers, func(a *notary.Actor) (*transaction.Transaction, error) {
		return a.MakeCall(calledContract, method, args...)
	})
}

// submitTransactionNotary constructs transaction.Transaction and wraps it into
// request for signing by a group of parties and spreads the request in the
// blockchain network. The action is performed via Neo Notary service. The
// request is signed on behalf of the specified simple Neo account.
func (x *Blockchain) submitTransactionNotary(simpleAcc *wallet.Account, signers []actor.SignerAccount, fTx func(*notary.Actor) (*transaction.Transaction, error)) error {
	a, err := notary.NewActor(x.rpcActor, signers, simpleAcc)
	if err != nil {
		return fmt.Errorf("init notary actor: %w", err)
	}

	_, _, _, err = a.Notarize(fTx(a))
	if err != nil {
		return fmt.Errorf("notarize transaction using notary actor: %w", err)
	}

	return nil
}

// Height returns sequence number of the latest block in the blockchain.
func (x *Blockchain) Height() uint32 {
	return x.base.BlockHeight()
}

// SubscribeToNotifications performs subscription to notification events spawned
// by Neo smart contracts and stream them into the returned channel as
// state.ContainedNotificationEvent instances. Caller must read the channel
// regularly in the background to prevent congestion. When notifications are no
// longer needed, caller should cancel subscription by calling the returned
// function.
//
// SubscribeToNotifications is expected to be called once since event
// multiplexing does not make sense.
func (x *Blockchain) SubscribeToNotifications() (<-chan *state.ContainedNotificationEvent, func()) {
	ch := make(chan *state.ContainedNotificationEvent)
	x.base.SubscribeForNotifications(ch)
	return ch, func() {
		x.base.UnsubscribeFromNotifications(ch)
	}
}

// SubscribeToMemPoolEvents behaves like SubscribeToNotifications but for
// mempool events.
func (x *Blockchain) SubscribeToMemPoolEvents() (<-chan mempoolevent.Event, func()) {
	ch := make(chan mempoolevent.Event)
	x.netServer.SubscribeForNotaryRequests(ch)
	return ch, func() {
		x.netServer.UnsubscribeFromNotaryRequests(ch)
	}
}

// NotaryServiceEnabled checks if Notary service is enabled in the related Neo
// blockchain.
func (x *Blockchain) NotaryServiceEnabled() bool {
	return x.base.P2PSigExtensionsEnabled()
}
