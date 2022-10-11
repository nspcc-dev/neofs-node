package innerring

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/internal/blockchain"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/models"
	neofsClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	containerSDK "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofsecdsa "github.com/nspcc-dev/neofs-sdk-go/crypto/ecdsa"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	subnetid "github.com/nspcc-dev/neofs-sdk-go/subnet/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

// blockChain provides services of the NeoFS-specific instances of the Neo
// blockchain networks consumed by the Inner Ring nodes for processing.
//
// All blockChain fields must be correctly set (newNode shows how it is embedded
// to the node). After initialization blockChain becomes a single-use component
// that can be started and then stopped. All operations should be executed after
// blockChain is started and before it is stopped (reverse behavior is
// undefined).
type blockChain struct {
	// Log of the internal events.
	log *logger.Logger

	// Table of the NeoFS smart contracts deployed in NeoFS blockchain network.
	contracts *contracts

	// Virtual connection to the NeoFS Sidechain network.
	sidechain *blockchain.Blockchain

	// NeoFS Mainchain-related info.
	mainChain struct {
		// Are NeoFS Mainchain-specific contracts (like NeoFS or Processing ones) are deployed
		// in the Neo blockchain network other than the NeoFS Sidechain.
		enabled bool
		// Virtual connection to eponymous with NeoFS smart contract.
		neoFSClient *neofsClient.Client
	}

	// Configured amount of the NeoFS Sidechain GAS emitted to all
	// storage nodes once per GAS emission cycle.
	storageEmissionAmount uint64
}

// run starts services of the underlying NeoFS blockchain and waits for all
// needed NeoFS contracts to be deployed in the NeoFS Sidechain and registered
// with particular names (*) in NNS contract also deployed in the NeoFS
// Sidechain with ID = 1. The waiting process is aborted by external signal read
// from the specified channel. Returns any error encountered which prevented the
// blockChain to be started. If run failed, the blockChain should no longer be
// used.
//
// (*) Pending contract names:
//   - 'container.neofs'
//   - 'audit.neofs'
//   - 'netmap.neofs'
//   - 'balance.neofs'
//   - 'neofsid.neofs'
//   - 'reputation.neofs'
//   - 'subnet.neofs'
//   - 'alphabetI.neofs' where 'I' is in range [0, size of the Sidechain committee)
//   - 'proxy.neofs' if Notary service is enabled in the NeoFS Sidechain network
//
// The run method should not be called more than once.
//
// Use stop method to stop the blockChain.
func (x *blockChain) run(chDone <-chan struct{}) error {
	err := x.sidechain.Run()
	if err != nil {
		return fmt.Errorf("run base blockchain component: %w", err)
	}

	committee, err := x.sidechain.Committee()
	if err != nil {
		return fmt.Errorf("get committee from the blockchain: %w", err)
	}

	awaitingContracts := []string{
		contractContainer.String(),
		contractAudit.String(),
		contractNetmap.String(),
		contractBalance.String(),
		contractNeoFSID.String(),
		contractReputation.String(),
		contractSubnet.String(),
	}

	for l := az; l < GlagoliticLetter(len(committee)); l++ {
		awaitingContracts = append(awaitingContracts, alphabetContractName(l).String())
	}

	if x.sidechain.NotaryServiceEnabled() {
		awaitingContracts = append(awaitingContracts, contractProxy.String())
	}

	x.log.Info("waiting for contracts to be deployed",
		zap.Strings("contracts", awaitingContracts),
	)

	err = x.sidechain.WaitForContracts(chDone, awaitingContracts, func(name string, addr util.Uint160) {
		x.contracts.set(contractName(name), addr)

		x.log.Info("system contract is available",
			zap.String("name", name),
			zap.String("address", addr.StringLE()),
		)
	})
	if err != nil {
		return fmt.Errorf("wait for NeoFS system contracts in the blockchain: %w", err)
	}

	x.log.Info("all contracts are in the blockchain")

	return nil
}

// stop stops the running blockChain and frees all its internal resources.
//
// The stop method should not be called twice and before successful run.
func (x *blockChain) stop() {
	x.sidechain.Stop()
}

// notification groups information from the notification event thrown by the Neo
// smart contract which is processed by the Inner Ring node.
type notification struct {
	// Name of the NeoFS contract which spawned the notification.
	contract contractName
	// Name of the notification.
	name string
	// Item stack of the notification payload.
	items []stackitem.Item
}

// subscribeToNotifications runs asynchronous process which listens to the
// NeoFS-related notifications thrown by Neo smart contracts and builds them
// into a synchronous queue of notification instances that can be read from the
// returned channel. Caller should regularly read elements from the queue to
// prevent congestion. Resulting channel is disposable: once it closed there
// will be no more requests from the blockchain. The listener is aborted by
// external signal read from the specified channel.
func (x *blockChain) subscribeToNotifications(chDone <-chan struct{}) <-chan notification {
	chSrc, cancel := x.sidechain.SubscribeToNotifications()
	chDst := make(chan notification)
	go x.distillNotifications(chSrc, chDst, chDone, cancel)
	return chDst
}

// distillNotifications reads raw notification events from the provided channel,
// filters ones spawned by the supported NeoFS smart contracts recorded in the
// underlying blockChain.contracts table and forms notification type instances
// which are written to the specified destination channel. Caller should
// regularly read it to prevent congestion.
//
// The distillNotifications is blocking: it's aborted when raw notification
// event channel is closed or external signal is read from the provided channel.
//
// When distillNotifications is finished, then destination channel is closed and
// cancel function is called.
func (x *blockChain) distillNotifications(chSrc <-chan *state.ContainedNotificationEvent, chDst chan<- notification, chDone <-chan struct{}, cancel func()) {
	defer func() {
		close(chDst)
		cancel()
	}()

	x.log.Info("distilling raw notifications from blockchain...")

	for {
		select {
		case <-chDone:
			x.log.Info("stopping distillation of blockchain notifications (external signal)")
			return
		case rawNotification, ok := <-chSrc:
			if !ok {
				x.log.Info("stopping distillation of blockchain notifications (channel of raw notifications is closed)")
				return
			}

			if rawNotification == nil {
				x.log.Warn("received unexpected nil notification from the channel")
				continue
			}

			contract, ok := x.contracts.resolve(rawNotification.ScriptHash)
			if !ok {
				x.log.Debug("ignore notification from unknown contract", zap.Stringer("address", rawNotification.ScriptHash))
				continue
			}

			chDst <- notification{
				name:     rawNotification.Name,
				contract: contract,
				items:    rawNotification.Item.Value().([]stackitem.Item), // panic is OK here
			}
		}
	}
}

// notaryRequest groups information from the request sent using Neo Notary
// service which is processed by the Inner Ring node.
type notaryRequest struct {
	// Main transaction from the request.
	mainTx transaction.Transaction
	// Name of the related NeoFS contract.
	contract contractName
	// Method of the contract calling in main transaction.
	method string
	// Neo VM opcodes related the method call.
	ops []event.Op
}

// subscribeToNotaryRequests runs asynchronous process which listens to the
// NeoFS-related requests sent using Neo Notary service and builds them into a
// synchronous queue of notaryRequest instances that can be read from the
// returned channel. Caller should regularly read elements from the queue to
// prevent congestion. Resulting channel is disposable: once it closed there
// will be no more requests from the blockchain. The listener is aborted by
// external signal read from the specified channel.
func (x *blockChain) subscribeToNotaryRequests(chDone <-chan struct{}) <-chan notaryRequest {
	chDst := make(chan notaryRequest)
	if x.sidechain.NotaryServiceEnabled() {
		chSrc, cancel := x.sidechain.SubscribeToMemPoolEvents()
		go x.distillNotaryRequests(chSrc, chDst, chDone, cancel)
	} else {
		close(chDst)
	}
	return chDst
}

// blockCounter implements event.BlockCounter through blockChain.
type blockCounter blockChain

// BlockCount returns sequence number of the latest block in the underlying
// blockChain.
func (x *blockCounter) BlockCount() (uint32, error) {
	return (*blockChain)(x).height()
}

// distillNotaryRequests reads raw mem-pool events from the provided channel,
// filters suitable (*) Neo Notary service's requests, verifies them and forms
// notaryRequest instances which are written to the specified destination
// channel. Caller should regularly read it to prevent congestion.
//
// (*) Selected mempool events:
//   - of type mempoolevent.TransactionAdded
//   - payload data of type payload.P2PNotaryRequest
//   - main transaction contains NeoFS Proxy contract as a signer
//   - main transaction has not been processed yet and valid
//   - main transaction relates to method call of one of the supported NeoFS
//     smart contracts recorded in the underlying blockChain.contracts table
//
// The distillNotaryRequests is blocking: it's aborted when mempool event
// channel is closed or external signal is read from the provided channel.
//
// When distillNotaryRequests is finished, then destination channel is closed
// and cancel function is called.
func (x *blockChain) distillNotaryRequests(chSrc <-chan mempoolevent.Event, chDst chan<- notaryRequest, chDone <-chan struct{}, cancel func()) {
	defer func() {
		close(chDst)
		cancel()
	}()

	preparer := event.NewNotaryPreparer(event.PreparatorPrm{
		AlphaKeys:    x.alphabetMembers,
		BlockCounter: (*blockCounter)(x),
	})

	x.log.Info("distilling raw notary requests from blockchain...")

	for {
		select {
		case <-chDone:
			x.log.Info("stopping distillation of blockchain notary requests (external signal)")
			return
		case rawEvent, ok := <-chSrc:
			if !ok {
				x.log.Info("stopping distillation of blockchain notary requests (channel of mempool events is closed)")
				return
			}

			if rawEvent.Type != mempoolevent.TransactionAdded {
				x.log.Debug("ignore mempool event by type", zap.Stringer("type", rawEvent.Type))
				continue
			}

			rawNotaryRequest, ok := rawEvent.Data.(*payload.P2PNotaryRequest)
			if !ok {
				x.log.Debug("ignore mempool event by payload type", zap.String("type", fmt.Sprintf("%T", rawEvent.Data)))
				continue
			}

			if rawNotaryRequest.MainTransaction == nil {
				x.log.Debug("ignore notary request (nil main transaction)")
				continue
			}

			if !rawNotaryRequest.MainTransaction.HasSigner(x.contracts.get(contractProxy)) {
				x.log.Debug("ignore notary request (missing Proxy contact signer)")
				continue
			}

			ev, err := preparer.Prepare(rawNotaryRequest)
			if err != nil {
				switch {
				case errors.Is(err, event.ErrTXAlreadyHandled):
					x.log.Debug("ignore notary request (main transaction has already been handled)")
				case errors.Is(err, event.ErrMainTXExpired):
					x.log.Debug("ignore notary request (main transaction is expired)")
				default:
					x.log.Error("ignore notary request (validation failed)", zap.Error(err))
				}

				continue
			}

			contract, ok := x.contracts.resolve(ev.ScriptHash())
			if !ok {
				x.log.Info("ignore notary request from unknown contract: %w", zap.Stringer("address", ev.ScriptHash()))
				continue
			}

			chDst <- notaryRequest{
				mainTx:   *rawNotaryRequest.MainTransaction,
				contract: contract,
				method:   ev.Type().String(),
				ops:      ev.Params(),
			}
		}
	}
}

// Returns keys.PublicKeys of the NeoFS Sidechain committee as NeoFS Alphabet
// members.
func (x *blockChain) alphabetMembers() (keys.PublicKeys, error) {
	// TODO: consider caching
	cmt, err := x.sidechain.Committee()
	if err != nil {
		return nil, fmt.Errorf("get committee list from the blockchain: %w", err)
	}

	return cmt, nil
}

// alphabetLetterForAccount returns GlagoliticLetter of the NeoFS Alphabet
// member for the specified wallet.Account. Returns negative value without an
// error if the account is not bound to any Alphabet member.
func (x *blockChain) alphabetLetterForAccount(acc *wallet.Account) (GlagoliticLetter, error) {
	key := acc.PublicKey()
	if key != nil {
		alphabetKeys, err := x.alphabetMembers()
		if err != nil {
			return 0, fmt.Errorf("form Alphabet list: %w", err)
		}

		for i := range alphabetKeys {
			if alphabetKeys[i].Equal(key) {
				return GlagoliticLetter(i), nil
			}
		}
	}

	return -1, nil
}

// alphabetContract returns address of the NeoFS Alphabet contract associated
// with the given GlagoliticLetter.
func (x *blockChain) alphabetContract(letter GlagoliticLetter) (res util.Uint160, err error) {
	res = getAlphabetContract(x.contracts, letter)
	if res == (util.Uint160{}) {
		// TODO: request from chain again?
		return res, fmt.Errorf("missing Alphabet contract %s", letter)
	}

	return res, nil
}

// callNamedSidechainContractMethod attempts to call specified method of the
// NeoFS Sidechain smart contract referenced by the given contractName with
// provided arguments and writes any failure to the log.
func (x *node) callNamedSidechainContractMethod(name contractName, method string, args ...interface{}) {
	err := x.sidechain.CallContractMethod(x.contracts.get(name), method, args...)
	if err != nil {
		x.log.Error("contract method call failed",
			zap.Error(err),
			zap.Stringer("name", name),
			zap.String("method", method),
		)
	}
}

var (
	errMissingNodeStructFields = errors.New("missing node struct fields")
	errMissingNodeBLOB         = errors.New("missing node BLOB")
)

// StorageNodes implements alphabet.NeoFS by calling 'netmap' method of the
// Netmap smart contract deployed in the NeoFS Sidechain.
func (x *blockChain) StorageNodes() ([]util.Uint160, error) {
	const method = "netmap"

	item, err := blockchain.GetContractMethodReturn(x.sidechain, x.contracts.get(contractNetmap), method)
	if err != nil {
		return nil, fmt.Errorf("call '%s' method of the Netmap contract: %w", method, err)
	}

	arr, err := blockchain.ToArrayNonEmpty(item, models.ErrEmptyNetmap)
	if err != nil {
		return nil, fmt.Errorf("convert Netmap contract '%s' method's return item to item array: %w", method, err)
	}

	res := make([]util.Uint160, len(arr))

	for i := range arr {
		nodeFields, err := blockchain.ToArrayNonEmpty(arr[i], errMissingNodeStructFields)
		if err != nil {
			return nil, fmt.Errorf("invalid item #%d in stack item array of nodes: %w", i, err)
		}

		nodeBLOB, err := blockchain.ToBytesNonEmpty(nodeFields[0], errMissingNodeBLOB)
		if err != nil {
			return nil, fmt.Errorf("invalid node BLOB field in node stack item #%d: %w", i, err)
		}

		// TODO: consider optimization of faster field access
		var node netmap.NodeInfo

		err = node.Unmarshal(nodeBLOB)
		if err != nil {
			return nil, fmt.Errorf("decode node #%d from BLOB: %w", i, err)
		}

		var pubKey neofsecdsa.PublicKey

		err = pubKey.Decode(node.PublicKey())
		if err != nil {
			return nil, fmt.Errorf("decode node #%d public key: %w", i, err)
		}

		res[i] = (*keys.PublicKey)(&pubKey).GetScriptHash()
	}

	return res, nil
}

// StorageEmissionAmount implements alphabet.NeoFS by reading corresponding configuration value.
func (x *blockChain) StorageEmissionAmount() (uint64, error) {
	return x.storageEmissionAmount, nil
}

// BalanceSystemPrecision implements balance.NeoFS by calling 'decimals' method of the NeoFS
// Balance smart contract deployed in the NeoFS Sidechain.
func (x *blockChain) BalanceSystemPrecision() (uint64, error) {
	const method = "decimals"

	item, err := blockchain.GetContractMethodReturn(x.sidechain, x.contracts.get(contractBalance), method)
	if err != nil {
		return 0, fmt.Errorf("call '%s' method of the Balance contract: %w", method, err)
	}

	n, err := item.TryInteger()
	if err != nil {
		return 0, fmt.Errorf("convert Balance contract '%s' method's return item to integer: %w", method, err)
	}

	return n.Uint64(), nil
}

// height returns sequence number of the latest block in the NeoFS Sidechain.
func (x *blockChain) height() (uint32, error) {
	return x.sidechain.Height(), nil
}

var errEmptyPublicKey = errors.New("empty public key")

// userKeys reads keys.PublicKeys bound to the user referenced by the given
// user.ID by calling 'key' method of the NeoFS ID smart contract
// deployed in the NeoFS Sidechain.
func (x *blockChain) userKeys(usr user.ID) (keys.PublicKeys, error) {
	const method = "key"

	item, err := blockchain.GetContractMethodReturn(x.sidechain, x.contracts.get(contractNeoFSID), "key", usr.WalletBytes())
	if err != nil {
		return nil, fmt.Errorf("call '%s' method of the NeoFSID contract: %w", method, err)
	}

	arr, err := blockchain.ToArray(item)
	if err != nil {
		return nil, fmt.Errorf("convert NeoFSID contract '%s' method's return item to array: %w", method, err)
	}

	res := make(keys.PublicKeys, len(arr))

	for i := range arr {
		rawKey, err := blockchain.ToBytesNonEmpty(arr[i], errEmptyPublicKey)
		if err != nil {
			return nil, fmt.Errorf("convert NeoFSID contract '%s' method's return array item #%d to bytes: %w", method, i, err)
		}

		res[i], err = keys.NewPublicKeyFromBytes(rawKey, elliptic.P256())
		if err != nil {
			return nil, fmt.Errorf("failed to decode public key from NeoFSID contract '%s' method's return array item #%d: %w", method, i, err)
		}
	}

	return res, nil
}

// Authorize implements container.NeoFS by verification of the digital data
// signature from the NeoFS user referenced by the given user.ID. The signature
// should pass verification by the neofsecdsa.PublicKeyRFC6979 parameter (if
// provided) or one of the user keys requested via calling 'key' method of the
// NeoFS ID smart contract deployed in the NeoFS Sidechain.
func (x *blockChain) Authorize(usr user.ID, data, signature []byte, key *neofsecdsa.PublicKeyRFC6979) error {
	if key != nil {
		// TODO(@cthulhu-rider): #1387 use another approach after neofs-sdk-go#233
		var idFromKey user.ID
		user.IDFromKey(&idFromKey, ecdsa.PublicKey(*key))

		if usr.Equals(idFromKey) {
			if key.Verify(data, signature) {
				return nil
			}

			return errors.New("incorrect signature")
		}
	}

	ks, err := x.userKeys(usr)
	if err != nil {
		return fmt.Errorf("get user's public keys: %w", err)
	}

	for i := range ks {
		if (*neofsecdsa.PublicKeyRFC6979)(ks[i]).Verify(data, signature) {
			return nil
		}
	}

	return errors.New("signature is incorrect or signed using unknown user key")
}

// CheckUserAllowanceToSubnet implements container.NeoFS by calling
// 'userAllowed' method of the NeoFS Subnet smart contract deployed in the NeoFS
// Sidechain.
func (x *blockChain) CheckUserAllowanceToSubnet(id subnetid.ID, usr user.ID) error {
	if subnetid.IsZero(id) {
		return nil
	}

	const method = "userAllowed"

	item, err := blockchain.GetContractMethodReturn(x.sidechain, x.contracts.get(contractSubnet), method, id.Marshal(), usr.WalletBytes())
	if err != nil {
		return fmt.Errorf("call '%s' method of the Subnet contract: %w", method, err)
	}

	b, err := item.TryBool()
	if err != nil {
		return fmt.Errorf("convert Subnet contract '%s' method's return item to boolean: %w", method, err)
	}

	if !b {
		return models.ErrSubnetAccess
	}

	return nil
}

var errMissingContainerBytes = errors.New("missing container bytes")

// getContainer reads containerSDK.Container referenced by the given cid.ID by calling 'get'
// method of the NeoFS Container smart contract deployed in the NeoFS Sidechain.
func (x *blockChain) getContainer(id cid.ID) (res containerSDK.Container, err error) {
	// TODO: consider caching
	bID := make([]byte, sha256.Size)
	id.Encode(bID)

	const method = "get"

	item, err := blockchain.GetContractMethodReturn(x.sidechain, x.contracts.get(contractContainer), "get", bID)
	if err != nil {
		return res, fmt.Errorf("call '%s' method of the Container contract: %w", method, err)
	}

	arr, err := blockchain.ToArray(item)
	if err != nil {
		return res, fmt.Errorf("convert Container contract '%s' method's return item to array: %w", method, err)
	} else if len(arr) == 0 {
		return res, fmt.Errorf("missing struct fields in the Container contract '%s' method's return item: %w", method, err)
	}

	b, err := blockchain.ToBytesNonEmpty(arr[0], errMissingContainerBytes)
	if err != nil {
		return res, fmt.Errorf("convert Container contract '%s' method's 1st struct field item to bytes: %w", method, err)
	}

	err = res.Unmarshal(b)
	if err != nil {
		return res, fmt.Errorf("decode container from 1st struct field of the Container contract '%s' method's return: %w", method, err)
	}

	return
}

// ContainerCreator implements container.NeoFS by reading container using 'get'
// method of the NeoFS Container smart contract deployed in the NeoFS Sidechain
// and accessing owner field of the result.
func (x *blockChain) ContainerCreator(id cid.ID) (res user.ID, err error) {
	cnr, err := x.getContainer(id)
	if err != nil {
		return res, fmt.Errorf("get container: %w", err)
	}

	return cnr.Owner(), nil
}

// IsContainerACLExtendable implements container.NeoFS by reading container using 'get'
// method of the NeoFS Container smart contract deployed in the NeoFS Sidechain
// and accessing corresponding property of the basic ACL field from the result.
func (x *blockChain) IsContainerACLExtendable(id cid.ID) (bool, error) {
	cnr, err := x.getContainer(id)
	if err != nil {
		return false, fmt.Errorf("get container: %w", err)
	}

	return cnr.BasicACL().Extendable(), nil
}

// CurrentEpoch implements container.NeoFS by calling 'epoch' method of the
// NeoFS Netmap smart contract deployed in the NeoFS Sidechain.
func (x *blockChain) CurrentEpoch() (uint64, error) {
	// TODO: consider caching
	const method = "epoch"

	item, err := blockchain.GetContractMethodReturn(x.sidechain, x.contracts.get(contractNetmap), method)
	if err != nil {
		return 0, fmt.Errorf("call '%s' method of the Netmap contract: %w", method, err)
	}

	n, err := item.TryInteger()
	if err != nil {
		return 0, fmt.Errorf("convert Netmap contract '%s' method's return item to integer: %w", method, err)
	}

	return n.Uint64(), nil
}

// networkParameter reads NeoFS network parameter value by calling 'config'
// method of the NeoFS Netmap smart contract deployed in the NeoFS Sidechain and
// returns the value as stackitem.Item. Returns nil without an error if
// requested parameter is missing in the configuration.
func (x *blockChain) networkParameter(key string) (stackitem.Item, error) {
	// TODO: consider caching
	const method = "config"

	item, err := blockchain.GetContractMethodReturn(x.sidechain, x.contracts.get(contractNetmap), method, []byte(key))
	if err != nil {
		return nil, fmt.Errorf("call '%s' method of the Netmap contract: %w", method, err)
	}

	if _, ok := item.(stackitem.Null); ok {
		return nil, nil
	}

	return item, nil
}

// IsHomomorphicHashingDisabled implements container.NeoFS by reading
// 'HomomorphicHashingDisabled' NeoFS network parameter via 'config' method of
// the NeoFS Netmap smart contract deployed in the NeoFS Sidechain. Returns
// false if corresponding parameter is missing.
func (x *blockChain) IsHomomorphicHashingDisabled() (bool, error) {
	const prm = "HomomorphicHashingDisabled"

	item, err := x.networkParameter(prm)
	if err != nil {
		return false, fmt.Errorf("get '%s' network parameter: %w", prm, err)
	} else if item == nil {
		return false, nil
	}

	b, err := item.TryBool()
	if err != nil {
		return false, fmt.Errorf("convert '%s' network parameter value item to bool: %w", prm, err)
	}

	return b, nil
}
