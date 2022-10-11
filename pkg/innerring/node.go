package innerring

import (
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/models"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/alphabet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/balance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/container"
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// node implements Inner Ring node which deals with the NeoFS-specific instances
// of the Neo blockchain networks. The node extends functionality of the public
// distributed NeoFS blockchain functionality with the operations done by the
// particular Inner Ring node.
//
// The node must be initialized using newNode constructor. After initialization
// node becomes a single-use component that can be started and then
// stopped. All operations should be executed after node is started and
// before it is stopped (reverse behavior is undefined).
type node struct {
	// node is an access point in the blockchain network.
	blockChain

	// Neo account of the related (local) Inner Ring member.
	acc *wallet.Account

	// Processors of the NeoFS blockchain events.
	processors struct {
		// processor of the Alphabet contract-related events.
		alphabet *alphabet.Processor
		// processor of the Balance contract-related events.
		balance *balance.Processor
		// processor of the Container contract-related events.
		container *container.Processor
	}
}

// mainChainStatus groups information about the main part of the NeoFS
// blockchain (Mainchain).
type mainChainStatus struct {
	// True when some NeoFS components are located in the separate (main) chain.
	separated bool

	// True when NeoFS Mainchain is separated in the Neo network with enabled Notary
	// service.
	notaryServiceEnabled bool
}

// newNode returns new node instance initialized using provided parameters. If
// newNode returns an error, node can (and must) not be used with provided
// configuration.
//
// Provided viper.Viper instance is expected to be a container of the Inner Ring
// application configuration. It must be previously read.
//
// Given wallet.Account is expected to be Inner Ring node's NEO account. It must
// be previously unlocked for signing.
//
// Given logger instance is used to log Inner Ring node events. It should be
// previously initialized. Neo blockchain-specific events are logged with
// 'Morph' name.
//
// Specified error channel is used to report internal node errors to the
// superior context listening to it in the background.
//
// If mainChainStatus.separated is true, then address of the NeoFS smart
// contract must be configured by 'contracts.neofs' path. In this case if
// mainChainStatus.notaryServiceEnabled is true, then address of the Processing
// contract must be configured by 'contracts.processing' path.
func newNode(cfg *viper.Viper, nodeAcc *wallet.Account, log *logger.Logger, chErr chan<- error, mainChain mainChainStatus) (*node, error) {
	var n node
	var err error

	n.sidechain, err = blockchain.New(cfg.ConfigFileUsed(), nodeAcc, log.Named("Morph"), chErr)
	if err != nil {
		return nil, fmt.Errorf("init blockchain module: %w", err)
	}

	const contractsConfigPrefix = "contracts."
	mConfigContracts := make(map[contractName]string)

	if mainChain.separated {
		mConfigContracts[contractNeoFS] = contractsConfigPrefix + "neofs"

		if mainChain.notaryServiceEnabled {
			mConfigContracts[contractProcessing] = contractsConfigPrefix + "processing"
		}
	}

	n.contracts, err = newContracts(cfg, mConfigContracts)
	if err != nil {
		return nil, fmt.Errorf("init blockchain contracts: %w", err)
	}

	n.storageEmissionAmount = cfg.GetUint64("emit.storage.amount")
	n.acc = nodeAcc

	n.processors.alphabet = alphabet.New(log, &n)
	n.processors.balance = balance.New(log, &n)
	n.processors.container = container.New(log, &n)

	n.mainChain.enabled = mainChain.separated
	n.log = log

	return &n, nil
}

// run starts underlying blockchain service and starts background process which
// listens to incoming events of the underlying blockchain and handles the
// NeoFS-specified ones. The listen process is aborted by the specified channel.
// Returns any error encountered which prevented the node to be started.
// If run failed, the node should no longer be used. After node has
// been successfully run, all internal failures are written to the channel
// provided to newNode.
//
// The run method should not be called more than once.
//
// Use stop method to stop the node.
func (x *node) run(chDone <-chan struct{}) error {
	err := x.blockChain.run(chDone)
	if err != nil {
		return fmt.Errorf("run node's blockchain basement: %w", err)
	}

	go x.listenBlockchain(chDone)

	return nil
}

// stop stops the running node and frees all its internal resources.
//
// The stop method should not be called twice and before successful run.
func (x *node) stop() {
	x.blockChain.stop()
}

// IsAlphabet implements common.NodeStatus via checking if local Inner Ring
// node's account belongs to someone from the NeoFS Sidechain committee.
func (x *node) IsAlphabet() (bool, error) {
	letter, err := x.alphabetLetterForAccount(x.acc)
	if err != nil {
		return false, err
	}

	return letter >= 0, nil
}

// EmitSidechainGAS implements alphabet.LocalNode via triggering production of
// the sidechain GAS and its distribution among Inner Ring nodes and Proxy
// contract by calling 'emit' method of the Alphabet contract associated with
// the local Inner Ring node.
func (x *node) EmitSidechainGAS() error {
	letter, err := x.alphabetLetterForAccount(x.acc)
	if err != nil {
		return err
	} else if letter < 0 {
		return models.ErrNonAlphabet
	}

	alphaContract, err := x.alphabetContract(letter)
	if err != nil {
		return err
	}

	const method = "emit" // just to avoid copy-paste

	err = x.sidechain.CallContractMethod(alphaContract, method)
	if err != nil {
		return fmt.Errorf("call local node's Alphabet contract method on blockchain (%s, %s): %w", alphaContract, method, err)
	}

	return nil
}

// TransferGAS implements alphabet.LocalNode via transferring specified amount
// of GAS from the local Inner Ring node's account to the given one by calling
// 'transfer' method of the Neo native GAS contract. Final transfer is not
// guaranteed.
//
// Amount MUST be positive.
func (x *node) TransferGAS(amount uint64, to util.Uint160) error {
	return x.sidechain.TransferGAS(amount, x.acc.ScriptHash(), to)
}

// buildAlphabetMultiSigAccount builds NeoFS Alphabet multi-signature
// wallet.Account from local Inner Ring node's account and specified public keys
// of the NeoFS Alphabet members with number of sufficient signatures set to
// the Alphabet size.
func (x *node) buildAlphabetMultiSigAccount(alphabetKeys keys.PublicKeys) (*wallet.Account, error) {
	acc := wallet.NewAccountFromPrivateKey(x.acc.PrivateKey())

	err := acc.ConvertMultisig(smartcontract.GetDefaultHonestNodeCount(len(alphabetKeys)), alphabetKeys)
	if err != nil {
		return nil, fmt.Errorf("convert node account to Alphabet multi-sig account: %w", err)
	}

	return acc, nil
}

// ApproveWithdrawal implements balance.LocalNode via calling the 'cheque'
// method of the NeoFS contract deployed in the NeoFS Mainchain (*) with
// following arguments:
//   - cheque ID set to the specified withdrawal transaction ID
//   - lock account to the first 20 bytes of the specified withdrawal transaction ID
//   - user account and assets' amount as it is
//
// If local node is configured with non-separated Mainchain, NeoFS contract is
// expected to be deployed in the NeoFS Sidechain.
func (x *node) ApproveWithdrawal(userAcc util.Uint160, amount uint64, withdrawTx util.Uint256) error {
	if x.mainChain.enabled {
		var lockAcc util.Uint160
		copy(lockAcc[:], withdrawTx[:])

		var prm neofscontract.ChequePrm
		prm.SetID(withdrawTx.BytesBE())
		prm.SetAmount(int64(amount))
		prm.SetUser(userAcc)
		prm.SetLock(lockAcc)
		prm.SetHash(withdrawTx)

		err := x.mainChain.neoFSClient.Cheque(prm)
		if err != nil {
			return fmt.Errorf("call cheque method of NeoFS contract client: %w", err)
		}

		return nil
	}

	const method = "cheque"
	contract := x.contracts.get(contractNeoFS)

	if !x.sidechain.NotaryServiceEnabled() {
		err := x.sidechain.CallContractMethod(contract, method, withdrawTx, userAcc, amount, withdrawTx)
		if err != nil {
			return fmt.Errorf("call '%s' method of the NeoFS contract: %w", method, err)
		}

		return nil
	}

	alphabetKeys, err := x.sidechain.Committee()
	if err != nil {
		return err
	}

	m := smartcontract.GetDefaultHonestNodeCount(len(alphabetKeys))

	multiSigScript, err := smartcontract.CreateMultiSigRedeemScript(m, alphabetKeys)
	if err != nil {
		return fmt.Errorf("create multi-sig redeem script for Alphabet: %w", err)
	}

	var tx transaction.Transaction

	// main net: &transaction.Signer{Scopes: transaction.CalledByEntry}
	// morph: &transaction.Signer{	Scopes: transaction.Global

	tx.Signers = []transaction.Signer{
		{
			Account: x.contracts.get(contractProxy),
			Scopes:  transaction.None,
		},
		{
			Account: hash.Hash160(multiSigScript),
			Scopes:  transaction.CalledByEntry, // transaction.Global for sidechain
		},
		{
			Account: notary.Hash, // notary contract
			Scopes:  transaction.None,
		},
	}

	callRes, err := x.sidechain.TestCallContractMethod(contract, method, withdrawTx, userAcc, amount, withdrawTx)
	if err != nil {
		return fmt.Errorf("test call '%s' method of the NeoFS contract: %w", method, err)
	}

	alphabetMultiSigAcc, err := x.buildAlphabetMultiSigAccount(alphabetKeys)
	if err != nil {
		return fmt.Errorf("build alphabet multi-sig account: %w", err)
	}

	withdrawHeight, err := x.sidechain.TransactionBlock(withdrawTx)
	if err != nil {
		return fmt.Errorf("get withdrawal transaction block height: %w", err)
	}

	tx.SystemFee = callRes.ConsumedGAS
	tx.Nonce = binary.BigEndian.Uint32(withdrawTx[:4])
	tx.ValidUntilBlock = withdrawHeight + 50
	tx.Script = callRes.Script
	tx.Attributes = []transaction.Attribute{{
		Type: transaction.NotaryAssistedT,
		Value: &transaction.NotaryAssisted{
			NKeys: uint8(len(alphabetKeys)),
		},
	}}
	tx.Scripts = []transaction.Witness{
		{ // placeholder for Proxy contract witness
			InvocationScript:   []byte{},
			VerificationScript: []byte{},
		},
		{
			InvocationScript:   x.sidechain.InvocationScriptOfTransactionSignature(alphabetMultiSigAcc, tx),
			VerificationScript: alphabetMultiSigAcc.GetVerificationScript(),
		},
		{ // placeholder for Notary contract witness
			InvocationScript:   append([]byte{byte(opcode.PUSHDATA1), 64}, make([]byte, 64)...),
			VerificationScript: []byte{},
		},
	}

	err = x.sidechain.SubmitTransactionNotary(tx, alphabetMultiSigAcc, x.acc)
	if err != nil {
		return fmt.Errorf("submit transaction to blockchain as notary request: %w", err)
	}

	return nil
}

func (x *node) signAndSendVerifiedNotaryRequest(n notaryRequest) {
	_alphabet, err := x.alphabetMembers()
	if err != nil {
		x.log.Error("compose list of alphabet members", zap.Error(err))
		return
	}

	alphabetMultiSigAcc, err := x.buildAlphabetMultiSigAccount(_alphabet)
	if err != nil {
		x.log.Error("failed to build Alphabet multi-sig account", zap.Error(err))
		return
	}

	err = x.sidechain.SignAndSendTransactionNotary(n.mainTx, alphabetMultiSigAcc, x.acc)
	if err != nil {
		x.log.Error("sign and send main transaction of the notary request", zap.Error(err))
		return
	}
}
