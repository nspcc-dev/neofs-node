// Package deploy provides NeoFS Sidechain deployment functionality.
package deploy

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/util/glagolitsa"
	"go.uber.org/zap"
)

// Blockchain groups services provided by particular Neo blockchain network
// representing NeoFS Sidechain that are required for its deployment.
type Blockchain interface {
	// RPCActor groups functions needed to compose and send transactions (incl.
	// Notary service requests) to the blockchain.
	notary.RPCActor

	// GetCommittee returns list of public keys owned by Neo blockchain committee
	// members. Resulting list is non-empty, unique and unsorted.
	GetCommittee() (keys.PublicKeys, error)

	// GetContractStateByID returns network state of the smart contract by its ID.
	// GetContractStateByID returns error with 'Unknown contract' substring if
	// requested contract is missing.
	GetContractStateByID(id int32) (*state.Contract, error)

	// GetContractStateByHash is similar to GetContractStateByID but accepts address.
	// GetContractStateByHash may return non-nil state.Contract along with an error.
	GetContractStateByHash(util.Uint160) (*state.Contract, error)

	// ReceiveBlocks starts background process that forwards new blocks of the
	// blockchain to the provided channel. The process handles all new blocks when
	// ReceiveBlocks is called with nil filter. Returns unique identifier to be used
	// to stop the process via Unsubscribe.
	ReceiveBlocks(*neorpc.BlockFilter, chan<- *block.Block) (id string, err error)

	// ReceiveNotaryRequests starts background process that forwards new notary
	// requests of the blockchain to the provided channel. The process skips
	// requests that don't match specified filter. Returns unique identifier to be
	// used to stop the process via Unsubscribe.
	ReceiveNotaryRequests(*neorpc.TxFilter, chan<- *result.NotaryRequestEvent) (string, error)

	// Unsubscribe stops background process started by ReceiveBlocks or
	// ReceiveNotaryRequests by ID.
	Unsubscribe(id string) error
}

// KeyStorage represents storage of the private keys.
type KeyStorage interface {
	// GetPersistedPrivateKey returns singleton private key persisted in the
	// storage. GetPersistedPrivateKey randomizes the key initially. All subsequent
	// successful calls return the same key.
	GetPersistedPrivateKey() (*keys.PrivateKey, error)
}

// NeoFSState groups information about NeoFS network state processed by Deploy.
type NeoFSState struct {
	// Current NeoFS epoch.
	CurrentEpoch uint64
	// Height of the NeoFS Sidechain at which CurrentEpoch began.
	CurrentEpochBlock uint32
	// Duration of the single NeoFS epoch measured in Sidechain blocks.
	EpochDuration uint32
}

// NeoFS provides access to the running NeoFS network.
type NeoFS interface {
	// CurrentState returns current state of the NeoFS network.
	CurrentState() (NeoFSState, error)
}

// CommonDeployPrm groups common deployment parameters of the smart contract.
type CommonDeployPrm struct {
	NEF      nef.File
	Manifest manifest.Manifest
}

// NNSPrm groups deployment parameters of the NeoFS NNS contract.
type NNSPrm struct {
	Common      CommonDeployPrm
	SystemEmail string
}

// AlphabetContractPrm groups deployment parameters of the NeoFS Alphabet contract.
type AlphabetContractPrm struct {
	Common CommonDeployPrm
}

// AuditContractPrm groups deployment parameters of the NeoFS Audit contract.
type AuditContractPrm struct {
	Common CommonDeployPrm
}

// BalanceContractPrm groups deployment parameters of the NeoFS Balance contract.
type BalanceContractPrm struct {
	Common CommonDeployPrm
}

// ContainerContractPrm groups deployment parameters of the Container contract.
type ContainerContractPrm struct {
	Common CommonDeployPrm
}

// NeoFSIDContractPrm groups deployment parameters of the NeoFS ID contract.
type NeoFSIDContractPrm struct {
	Common CommonDeployPrm
}

// NetmapContractPrm groups deployment parameters of the Netmap contract.
type NetmapContractPrm struct {
	Common CommonDeployPrm
	Config netmap.NetworkConfiguration
}

// ProxyContractPrm groups deployment parameters of the NeoFS Proxy contract.
type ProxyContractPrm struct {
	Common CommonDeployPrm
}

// ReputationContractPrm groups deployment parameters of the NeoFS Reputation contract.
type ReputationContractPrm struct {
	Common CommonDeployPrm
}

// Prm groups all parameters of the NeoFS Sidechain deployment procedure.
type Prm struct {
	// Writes progress into the log.
	Logger *zap.Logger

	// Particular Neo blockchain instance to be used as NeoFS Sidechain.
	Blockchain Blockchain

	// Local process account used for transaction signing (must be unlocked).
	LocalAccount *wallet.Account

	// Validator multi-sig account to spread initial GAS to network
	// participants (must be unlocked).
	ValidatorMultiSigAccount *wallet.Account

	// Storage for single committee group key.
	KeyStorage KeyStorage

	// Running NeoFS network for which deployment procedure is performed.
	NeoFS NeoFS

	NNS NNSPrm

	AlphabetContract   AlphabetContractPrm
	AuditContract      AuditContractPrm
	BalanceContract    BalanceContractPrm
	ContainerContract  ContainerContractPrm
	NeoFSIDContract    NeoFSIDContractPrm
	NetmapContract     NetmapContractPrm
	ProxyContract      ProxyContractPrm
	ReputationContract ReputationContractPrm
}

// Deploy initializes Neo network represented by given Prm.Blockchain as NeoFS
// Sidechain and makes it full-featured for NeoFS storage system operation.
//
// Deploy aborts only by context or when a fatal error occurs. Deployment
// progress is logged in detail. It is expected that some situations can be
// changed/fixed on the chain from the outside, so Deploy adapts flexibly and
// does not stop at the moment.
//
// Deployment process is detailed in NeoFS docs. Summary of stages:
//  1. NNS contract deployment
//  2. launch of a notary service for the committee
//  3. initial GAS distribution between committee members
//  4. committee group initialization
//  5. Alphabet initialization (incl. registration as candidates to validators)
//  6. deployment/update of the NeoFS system contracts
//  7. distribution of all available NEO between the Alphabet contracts
//
// See project documentation for details.
func Deploy(ctx context.Context, prm Prm) error {
	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	committee, err := prm.Blockchain.GetCommittee()
	if err != nil {
		return fmt.Errorf("get Neo committee of the network: %w", err)
	}

	sort.Sort(committee)

	// determine a leader
	localPrivateKey := prm.LocalAccount.PrivateKey()
	localPublicKey := localPrivateKey.PublicKey()
	localAccCommitteeIndex := -1

	for i := range committee {
		if committee[i].Equal(localPublicKey) {
			localAccCommitteeIndex = i
			break
		}
	}

	if localAccCommitteeIndex < 0 {
		return errors.New("local account does not belong to any Neo committee member")
	}

	simpleLocalActor, err := actor.NewSimple(prm.Blockchain, prm.LocalAccount)
	if err != nil {
		return fmt.Errorf("init transaction sender from single local account: %w", err)
	}

	committeeLocalActor, err := newCommitteeNotaryActor(prm.Blockchain, prm.LocalAccount, committee)
	if err != nil {
		return fmt.Errorf("create Notary service client sending transactions to be signed by the committee: %w", err)
	}

	chNewBlock := make(chan struct{}, 1)

	monitor, err := newBlockchainMonitor(prm.Logger, prm.Blockchain, chNewBlock)
	if err != nil {
		return fmt.Errorf("init blockchain monitor: %w", err)
	}

	defer monitor.stop()

	deployNNSPrm := deployNNSContractPrm{
		logger:                prm.Logger,
		blockchain:            prm.Blockchain,
		monitor:               monitor,
		localAcc:              prm.LocalAccount,
		localNEF:              prm.NNS.Common.NEF,
		localManifest:         prm.NNS.Common.Manifest,
		systemEmail:           prm.NNS.SystemEmail,
		initCommitteeGroupKey: nil, // set below
	}

	// if local node is the first committee member (Az) => deploy NNS contract,
	// otherwise just wait
	if localAccCommitteeIndex == 0 {
		// Why such a centralized approach? There is a need to initialize committee
		// contract group and share its private key between all committee members (the
		// latter is done in the current procedure next). Currently, there is no
		// convenient Neo service for this, and we don't want to use anything but
		// blockchain, so the key is distributed through domain NNS records. However,
		// then the chicken-and-egg problem pops up: committee group must be also set
		// for the NNS contract. To set the group, you need to know the contract hash in
		// advance, and it is a function from the sender of the deployment transaction.
		// Summing up all these statements, we come to the conclusion that the one who
		// deploys the contract creates the group key, and he shares it among the other
		// members. Technically any committee member could deploy NNS contract, but for
		// the sake of simplicity, this is a fixed node. This makes the procedure even
		// more centralized, however, in practice, at the start of the network, all
		// members are expected to be healthy and active.
		//
		// Note that manifest can't be changed w/o NEF change, so it's impossible to set
		// committee group dynamically right after deployment. See
		// https://github.com/nspcc-dev/neofs-contract/issues/340
		deployNNSPrm.initCommitteeGroupKey = prm.KeyStorage.GetPersistedPrivateKey
	}

	prm.Logger.Info("initializing NNS contract on the chain...")

	nnsOnChainAddress, err := initNNSContract(ctx, deployNNSPrm)
	if err != nil {
		return fmt.Errorf("init NNS contract on the chain: %w", err)
	}

	prm.Logger.Info("NNS contract successfully initialized on the chain", zap.Stringer("address", nnsOnChainAddress))

	prm.Logger.Info("enable Notary service for the committee...")

	err = enableNotary(ctx, enableNotaryPrm{
		logger:                 prm.Logger,
		blockchain:             prm.Blockchain,
		monitor:                monitor,
		nnsOnChainAddress:      nnsOnChainAddress,
		systemEmail:            prm.NNS.SystemEmail,
		committee:              committee,
		localAcc:               prm.LocalAccount,
		localAccCommitteeIndex: localAccCommitteeIndex,
	})
	if err != nil {
		return fmt.Errorf("enable Notary service for the committee: %w", err)
	}

	prm.Logger.Info("Notary service successfully enabled for the committee")

	go autoReplenishNotaryBalance(ctx, prm.Logger, prm.Blockchain, prm.LocalAccount, chNewBlock)

	err = listenCommitteeNotaryRequests(ctx, listenCommitteeNotaryRequestsPrm{
		logger:               prm.Logger,
		blockchain:           prm.Blockchain,
		localAcc:             prm.LocalAccount,
		committee:            committee,
		validatorMultiSigAcc: prm.ValidatorMultiSigAccount,
	})
	if err != nil {
		return fmt.Errorf("start listener of committee notary requests: %w", err)
	}

	prm.Logger.Info("making initial transfer of funds to the committee...")

	err = makeInitialTransferToCommittee(ctx, makeInitialGASTransferToCommitteePrm{
		logger:               prm.Logger,
		blockchain:           prm.Blockchain,
		monitor:              monitor,
		committee:            committee,
		localAcc:             prm.LocalAccount,
		validatorMultiSigAcc: prm.ValidatorMultiSigAccount,
		tryTransfer:          localAccCommitteeIndex == 0,
	})
	if err != nil {
		return fmt.Errorf("initial transfer funds to the committee: %w", err)
	}

	prm.Logger.Info("initial transfer to the committee successfully done")

	prm.Logger.Info("initializing committee group for contract management...")

	committeeGroupKey, err := initCommitteeGroup(ctx, initCommitteeGroupPrm{
		logger:                 prm.Logger,
		blockchain:             prm.Blockchain,
		monitor:                monitor,
		nnsOnChainAddress:      nnsOnChainAddress,
		systemEmail:            prm.NNS.SystemEmail,
		committee:              committee,
		localAcc:               prm.LocalAccount,
		localAccCommitteeIndex: localAccCommitteeIndex,
		keyStorage:             prm.KeyStorage,
	})
	if err != nil {
		return fmt.Errorf("init committee group: %w", err)
	}

	prm.Logger.Info("committee group successfully initialized", zap.Stringer("public key", committeeGroupKey.PublicKey()))

	prm.Logger.Info("registering committee group in the NNS...")

	err = registerCommitteeGroupInNNS(ctx, registerCommitteeGroupInNNSPrm{
		logger:            prm.Logger,
		blockchain:        prm.Blockchain,
		monitor:           monitor,
		nnsContract:       nnsOnChainAddress,
		systemEmail:       prm.NNS.SystemEmail,
		localAcc:          prm.LocalAccount,
		committee:         committee,
		committeeGroupKey: committeeGroupKey,
	})
	if err != nil {
		return fmt.Errorf("regsiter committee group in the NNS: %w", err)
	}

	prm.Logger.Info("committee group successfully registered in the NNS")

	prm.Logger.Info("initializing NeoFS Alphabet...")

	err = initAlphabet(ctx, initAlphabetPrm{
		logger:     prm.Logger,
		blockchain: prm.Blockchain,
		monitor:    monitor,
		committee:  committee,
		localAcc:   prm.LocalAccount,
	})
	if err != nil {
		return fmt.Errorf("init NeoFS Alphabet: %w", err)
	}

	prm.Logger.Info("NeoFS Alphabet successfully initialized")

	syncPrm := syncNeoFSContractPrm{
		logger:              prm.Logger,
		blockchain:          prm.Blockchain,
		neoFS:               prm.NeoFS,
		monitor:             monitor,
		localAcc:            prm.LocalAccount,
		nnsContract:         nnsOnChainAddress,
		systemEmail:         prm.NNS.SystemEmail,
		committee:           committee,
		committeeGroupKey:   committeeGroupKey,
		simpleLocalActor:    simpleLocalActor,
		committeeLocalActor: committeeLocalActor,
	}

	localAccLeads := localAccCommitteeIndex == 0

	// we attempt to deploy contracts (except Alphabet ones) by single committee
	// member (1st for simplicity) to reduce the likelihood of contract duplication
	// in the chain and better predictability of the final address (the address is a
	// function from the sender of the deploying transaction). While this approach
	// is centralized, we still expect any node incl. 1st one to be "healthy".
	// Updates are done concurrently.
	syncPrm.tryDeploy = localAccLeads

	var notaryDisabledExtraUpdateArg bool

	// function allowing to calculate addresses of cross-dependent contracts. For
	// example, when A contract requires address of the B one, and B contract
	// requires address of the A one, we cannot get on-chain addresses of them both
	// because it's a cross dependency. Since fixed account performs initial
	// deployment (see above why), we are able to pre-calculate addresses and
	// resolve dependency problem.
	//
	// Things may become better/easier after
	// https://github.com/nspcc-dev/neofs-contract/issues/325
	resolveContractAddressDynamically := func(commonPrm CommonDeployPrm, contractDomain string) (util.Uint160, error) {
		domain := calculateContractAddressDomain(contractDomain)
		onChainState, err := readContractOnChainStateByDomainName(prm.Blockchain, nnsOnChainAddress, domain)
		if err != nil {
			// contract may be deployed but not registered in the NNS yet
			if localAccLeads && (errors.Is(err, errMissingDomain) || errors.Is(err, errMissingDomainRecord)) {
				return state.CreateContractHash(prm.LocalAccount.ScriptHash(), commonPrm.NEF.Checksum, commonPrm.Manifest.Name), nil
			}
			return util.Uint160{}, fmt.Errorf("failed to read on-chain state of the contract by NNS domain '%s': %w", domain, err)
		}
		return onChainState.Hash, nil
	}

	// Deploy NeoFS contracts in strict order. Contracts dependent on others come
	// after.

	// 1. Proxy
	//
	// It's required for Notary service to work, and also pays for subsequent
	// contract updates.
	syncPrm.localNEF = prm.ProxyContract.Common.NEF
	syncPrm.localManifest = prm.ProxyContract.Common.Manifest
	syncPrm.domainName = domainProxy
	syncPrm.buildExtraDeployArgs = noExtraDeployArgs
	syncPrm.buildExtraUpdateArgs = noExtraUpdateArgs
	syncPrm.isProxy = true

	prm.Logger.Info("synchronizing Proxy contract with the chain...")

	proxyContractAddress, err := syncNeoFSContract(ctx, syncPrm)
	if err != nil {
		return fmt.Errorf("sync Proxy contract with the chain: %w", err)
	}

	prm.Logger.Info("Proxy contract successfully synchronized", zap.Stringer("address", proxyContractAddress))

	// use on-chain address of the Proxy contract to update all others
	syncPrm.isProxy = false
	syncPrm.proxyContract = proxyContractAddress

	prm.Logger.Info("replenishing the the Proxy contract's balance...")

	err = transferGASToProxy(ctx, transferGASToProxyPrm{
		logger:        prm.Logger,
		blockchain:    prm.Blockchain,
		monitor:       monitor,
		proxyContract: proxyContractAddress,
		committee:     committee,
		localAcc:      prm.LocalAccount,
		tryTransfer:   localAccLeads,
	})
	if err != nil {
		return fmt.Errorf("replenish balance of the Proxy contract: %w", err)
	}

	prm.Logger.Info("Proxy balance successfully replenished")

	prm.Logger.Info("initializing vote for NeoFS Alphabet members to role of validators...")

	err = initVoteForAlphabet(ctx, initVoteForAlphabetPrm{
		logger:        prm.Logger,
		blockchain:    prm.Blockchain,
		monitor:       monitor,
		committee:     committee,
		localAcc:      prm.LocalAccount,
		proxyContract: proxyContractAddress,
	})
	if err != nil {
		return fmt.Errorf("init vote for NeoFS Alphabet members to role of validators: %w", err)
	}

	prm.Logger.Info("vote for NeoFS Alphabet to role of validators successfully initialized")

	// NNS (update)
	//
	// Special contract which is always deployed first, but its update depends on
	// Proxy contract.
	prm.Logger.Info("updating on-chain NNS contract...")

	err = updateNNSContract(ctx, updateNNSContractPrm{
		logger:               prm.Logger,
		blockchain:           prm.Blockchain,
		neoFS:                prm.NeoFS,
		monitor:              monitor,
		localAcc:             prm.LocalAccount,
		localNEF:             prm.NNS.Common.NEF,
		localManifest:        prm.NNS.Common.Manifest,
		systemEmail:          prm.NNS.SystemEmail,
		committee:            committee,
		committeeGroupKey:    committeeGroupKey,
		buildExtraUpdateArgs: noExtraUpdateArgs,
		proxyContract:        proxyContractAddress,
	})
	if err != nil {
		return fmt.Errorf("update NNS contract on the chain: %w", err)
	}

	prm.Logger.Info("on-chain NNS contract successfully updated")

	// 2. Audit
	syncPrm.localNEF = prm.AuditContract.Common.NEF
	syncPrm.localManifest = prm.AuditContract.Common.Manifest
	syncPrm.domainName = domainAudit
	syncPrm.buildExtraDeployArgs = noExtraDeployArgs
	syncPrm.buildExtraUpdateArgs = func() ([]interface{}, error) {
		return []interface{}{notaryDisabledExtraUpdateArg}, nil
	}

	prm.Logger.Info("synchronizing Audit contract with the chain...")

	auditContractAddress, err := syncNeoFSContract(ctx, syncPrm)
	if err != nil {
		return fmt.Errorf("sync Audit contract with the chain: %w", err)
	}

	prm.Logger.Info("Audit contract successfully synchronized", zap.Stringer("address", auditContractAddress))

	// 3. Balance
	syncPrm.localNEF = prm.BalanceContract.Common.NEF
	syncPrm.localManifest = prm.BalanceContract.Common.Manifest
	syncPrm.domainName = domainBalance
	syncPrm.buildExtraDeployArgs = noExtraDeployArgs
	syncPrm.buildExtraUpdateArgs = func() ([]interface{}, error) {
		return []interface{}{notaryDisabledExtraUpdateArg}, nil
	}

	prm.Logger.Info("synchronizing Balance contract with the chain...")

	balanceContractAddress, err := syncNeoFSContract(ctx, syncPrm)
	if err != nil {
		return fmt.Errorf("sync Balance contract with the chain: %w", err)
	}

	prm.Logger.Info("Balance contract successfully synchronized", zap.Stringer("address", balanceContractAddress))

	// 4. Reputation
	syncPrm.localNEF = prm.ReputationContract.Common.NEF
	syncPrm.localManifest = prm.ReputationContract.Common.Manifest
	syncPrm.domainName = domainReputation
	syncPrm.buildExtraDeployArgs = noExtraDeployArgs
	syncPrm.buildExtraUpdateArgs = func() ([]interface{}, error) {
		return []interface{}{notaryDisabledExtraUpdateArg}, nil
	}

	prm.Logger.Info("synchronizing Reputation contract with the chain...")

	reputationContractAddress, err := syncNeoFSContract(ctx, syncPrm)
	if err != nil {
		return fmt.Errorf("sync Reputation contract with the chain: %w", err)
	}

	prm.Logger.Info("Reputation contract successfully synchronized", zap.Stringer("address", reputationContractAddress))

	// order of the following contracts is trickier:
	//  - Netmap depends on Container
	//  - NeoFS ID depends on Netmap
	//  - Container depends on Netmap and NeoFS ID
	// (other dependencies doesn't matter in current context)
	//
	// according to this, we cannot select linear deployment order, so, taking
	// into account we use workaround described above, the order is any

	// 5. NeoFSID
	syncPrm.localNEF = prm.NeoFSIDContract.Common.NEF
	syncPrm.localManifest = prm.NeoFSIDContract.Common.Manifest
	syncPrm.domainName = domainNeoFSID
	syncPrm.buildExtraDeployArgs = func() ([]interface{}, error) {
		netmapContractAddress, err := resolveContractAddressDynamically(prm.NetmapContract.Common, domainNetmap)
		if err != nil {
			return nil, fmt.Errorf("resolve address of the Netmap contract: %w", err)
		}
		return []interface{}{
			notaryDisabledExtraUpdateArg,
			netmapContractAddress,
		}, nil
	}
	syncPrm.buildExtraUpdateArgs = func() ([]interface{}, error) {
		return []interface{}{notaryDisabledExtraUpdateArg}, nil
	}

	prm.Logger.Info("synchronizing NeoFSID contract with the chain...")

	neoFSIDContractAddress, err := syncNeoFSContract(ctx, syncPrm)
	if err != nil {
		return fmt.Errorf("sync NeoFSID contract with the chain: %w", err)
	}

	prm.Logger.Info("NeoFSID contract successfully synchronized", zap.Stringer("address", neoFSIDContractAddress))

	// 6. Container
	syncPrm.localNEF = prm.ContainerContract.Common.NEF
	syncPrm.localManifest = prm.ContainerContract.Common.Manifest
	syncPrm.domainName = domainContainer
	syncPrm.committeeDeployRequired = true
	syncPrm.buildExtraDeployArgs = func() ([]interface{}, error) {
		netmapContractAddress, err := resolveContractAddressDynamically(prm.NetmapContract.Common, domainNetmap)
		if err != nil {
			return nil, fmt.Errorf("resolve address of the Netmap contract: %w", err)
		}
		return []interface{}{
			notaryDisabledExtraUpdateArg,
			netmapContractAddress,
			balanceContractAddress,
			neoFSIDContractAddress,
			nnsOnChainAddress,
			domainContainers,
		}, nil
	}
	syncPrm.buildExtraUpdateArgs = func() ([]interface{}, error) {
		return []interface{}{notaryDisabledExtraUpdateArg}, nil
	}

	prm.Logger.Info("synchronizing Container contract with the chain...")

	containerContractAddress, err := syncNeoFSContract(ctx, syncPrm)
	if err != nil {
		return fmt.Errorf("sync Container contract with the chain: %w", err)
	}

	prm.Logger.Info("Container contract successfully synchronized", zap.Stringer("address", containerContractAddress))

	syncPrm.committeeDeployRequired = false

	// 7. Netmap
	netConfig := []interface{}{
		[]byte(netmap.MaxObjectSizeConfig), encodeUintConfig(prm.NetmapContract.Config.MaxObjectSize),
		[]byte(netmap.BasicIncomeRateConfig), encodeUintConfig(prm.NetmapContract.Config.StoragePrice),
		[]byte(netmap.AuditFeeConfig), encodeUintConfig(prm.NetmapContract.Config.AuditFee),
		[]byte(netmap.EpochDurationConfig), encodeUintConfig(prm.NetmapContract.Config.EpochDuration),
		[]byte(netmap.ContainerFeeConfig), encodeUintConfig(prm.NetmapContract.Config.ContainerFee),
		[]byte(netmap.ContainerAliasFeeConfig), encodeUintConfig(prm.NetmapContract.Config.ContainerAliasFee),
		[]byte(netmap.EigenTrustIterationsConfig), encodeUintConfig(prm.NetmapContract.Config.EigenTrustIterations),
		[]byte(netmap.EigenTrustAlphaConfig), encodeFloatConfig(prm.NetmapContract.Config.EigenTrustAlpha),
		[]byte(netmap.InnerRingCandidateFeeConfig), encodeUintConfig(prm.NetmapContract.Config.IRCandidateFee),
		[]byte(netmap.WithdrawFeeConfig), encodeUintConfig(prm.NetmapContract.Config.WithdrawalFee),
		[]byte(netmap.HomomorphicHashingDisabledKey), encodeBoolConfig(prm.NetmapContract.Config.HomomorphicHashingDisabled),
		[]byte(netmap.MaintenanceModeAllowedConfig), encodeBoolConfig(prm.NetmapContract.Config.MaintenanceModeAllowed),
	}

	for i := range prm.NetmapContract.Config.Raw {
		netConfig = append(netConfig, []byte(prm.NetmapContract.Config.Raw[i].Name), prm.NetmapContract.Config.Raw[i].Value)
	}

	syncPrm.localNEF = prm.NetmapContract.Common.NEF
	syncPrm.localManifest = prm.NetmapContract.Common.Manifest
	syncPrm.domainName = domainNetmap
	syncPrm.buildExtraDeployArgs = func() ([]interface{}, error) {
		return []interface{}{
			notaryDisabledExtraUpdateArg,
			balanceContractAddress,
			containerContractAddress,
			[]interface{}(nil), // keys, currently unused
			netConfig,
		}, nil
	}
	syncPrm.buildExtraUpdateArgs = func() ([]interface{}, error) {
		return []interface{}{notaryDisabledExtraUpdateArg, util.Uint160{}, util.Uint160{}, []interface{}(nil), []interface{}(nil)}, nil
	}

	prm.Logger.Info("synchronizing Netmap contract with the chain...")

	netmapContractAddress, err := syncNeoFSContract(ctx, syncPrm)
	if err != nil {
		return fmt.Errorf("sync Netmap contract with the chain: %w", err)
	}

	prm.Logger.Info("Netmap contract successfully synchronized", zap.Stringer("address", netmapContractAddress))

	// 8. Alphabet
	syncPrm.localNEF = prm.AlphabetContract.Common.NEF
	syncPrm.localManifest = prm.AlphabetContract.Common.Manifest
	syncPrm.buildExtraUpdateArgs = func() ([]interface{}, error) {
		return []interface{}{notaryDisabledExtraUpdateArg}, nil
	}

	var alphabetContracts []util.Uint160

	for ind := 0; ind < len(committee) && ind < glagolitsa.Size; ind++ {
		syncPrm.tryDeploy = ind == localAccCommitteeIndex // each member deploys its own Alphabet contract
		syncPrm.domainName = calculateAlphabetContractAddressDomain(ind)
		syncPrm.buildExtraDeployArgs = func() ([]interface{}, error) {
			return []interface{}{
				notaryDisabledExtraUpdateArg,
				netmapContractAddress,
				proxyContractAddress,
				glagolitsa.LetterByIndex(ind),
				ind,
				len(committee),
			}, nil
		}

		prm.Logger.Info("synchronizing Alphabet contract with the chain...", zap.Int("index", ind))

		alphabetContractAddress, err := syncNeoFSContract(ctx, syncPrm)
		if err != nil {
			return fmt.Errorf("sync Alphabet contract #%d with the chain: %w", ind, err)
		}

		prm.Logger.Info("Alphabet contract successfully synchronized",
			zap.Int("index", ind), zap.Stringer("address", alphabetContractAddress))

		alphabetContracts = append(alphabetContracts, alphabetContractAddress)
	}

	prm.Logger.Info("distributing NEO to the Alphabet contracts...")

	err = distributeNEOToAlphabetContracts(ctx, distributeNEOToAlphabetContractsPrm{
		logger:            prm.Logger,
		blockchain:        prm.Blockchain,
		monitor:           monitor,
		proxyContract:     proxyContractAddress,
		committee:         committee,
		localAcc:          prm.LocalAccount,
		alphabetContracts: alphabetContracts,
	})
	if err != nil {
		return fmt.Errorf("distribute NEO to the Alphabet contracts: %w", err)
	}

	prm.Logger.Info("NEO distribution to the Alphabet contracts successfully completed")

	return nil
}

func noExtraUpdateArgs() ([]interface{}, error) { return nil, nil }

func noExtraDeployArgs() ([]interface{}, error) { return nil, nil }

func encodeUintConfig(v uint64) []byte {
	return stackitem.NewBigInteger(new(big.Int).SetUint64(v)).Bytes()
}

func encodeFloatConfig(v float64) []byte {
	return []byte(strconv.FormatFloat(v, 'f', -1, 64))
}

func encodeBoolConfig(v bool) []byte {
	return stackitem.NewBool(v).Bytes()
}

// returns actor.TransactionCheckerModifier which sets current NeoFS epoch as
// nonce of the transaction and makes it valid 100 blocks after Sidechain block
// when the epoch began.
func neoFSRuntimeTransactionModifier(neoFS NeoFS) actor.TransactionCheckerModifier {
	return func(r *result.Invoke, tx *transaction.Transaction) error {
		err := actor.DefaultCheckerModifier(r, tx)
		if err != nil {
			return err
		}

		neoFSState, err := neoFS.CurrentState()
		if err != nil {
			return fmt.Errorf("get current NeoFS network state: %w", err)
		}

		tx.Nonce = uint32(neoFSState.CurrentEpoch)
		if math.MaxUint32-neoFSState.CurrentEpochBlock > neoFSState.EpochDuration {
			tx.ValidUntilBlock = neoFSState.CurrentEpochBlock + neoFSState.EpochDuration
		} else {
			tx.ValidUntilBlock = math.MaxUint32
		}

		return nil
	}
}
