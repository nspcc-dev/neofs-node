// Package deploy provides NeoFS Sidechain deployment functionality.
package deploy

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

// Blockchain groups services provided by particular Neo blockchain network
// representing NeoFS Sidechain that are required for its deployment.
type Blockchain interface {
	// RPCActor groups functions needed to compose and send transactions to the
	// blockchain.
	actor.RPCActor

	// GetCommittee returns list of public keys owned by Neo blockchain committee
	// members. Resulting list is non-empty, unique and unsorted.
	GetCommittee() (keys.PublicKeys, error)

	// GetContractStateByID returns network state of the smart contract by its ID.
	// GetContractStateByID returns error with 'Unknown contract' substring if
	// requested contract is missing.
	GetContractStateByID(id int32) (*state.Contract, error)

	// ReceiveBlocks starts background process that forwards new blocks of the
	// blockchain to the provided channel. The process handles all new blocks when
	// ReceiveBlocks is called with nil filter. Returns unique identifier to be used
	// to stop the process via Unsubscribe.
	ReceiveBlocks(*neorpc.BlockFilter, chan<- *block.Block) (id string, err error)

	// Unsubscribe stops background process started by ReceiveBlocks by ID.
	Unsubscribe(id string) error
}

// KeyStorage represents storage of the private keys.
type KeyStorage interface {
	// GetPersistedPrivateKey returns singleton private key persisted in the
	// storage. GetPersistedPrivateKey randomizes the key initially. All subsequent
	// successful calls return the same key.
	GetPersistedPrivateKey() (*keys.PrivateKey, error)
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

// Prm groups all parameters of the NeoFS Sidechain deployment procedure.
type Prm struct {
	// Writes progress into the log.
	Logger *zap.Logger

	// Particular Neo blockchain instance to be used as NeoFS Sidechain.
	Blockchain Blockchain

	// Local process account used for transaction signing (must be unlocked).
	LocalAccount *wallet.Account

	// Storage for single committee group key.
	KeyStorage KeyStorage

	NNS NNSPrm
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
//  3. committee group initialization
//  4. deployment of the NeoFS system contracts (currently not done)
//  5. deployment of custom contracts
//
// See project documentation for details.
func Deploy(ctx context.Context, prm Prm) error {
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

	deployNNSPrm := deployNNSContractPrm{
		logger:                prm.Logger,
		blockchain:            prm.Blockchain,
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

	prm.Logger.Info("initializing committee group for contract management...")

	committeeGroupKey, err := initCommitteeGroup(ctx, initCommitteeGroupPrm{
		logger:                 prm.Logger,
		blockchain:             prm.Blockchain,
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

	// TODO: deploy contracts

	return nil
}
