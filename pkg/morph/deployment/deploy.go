package deployment

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

type Blockchain interface {
	notary.RPCActor
	GetCommittee() (keys.PublicKeys, error)
	GetContractStateByID(id int32) (*state.Contract, error)
	ReceiveBlocks(*neorpc.BlockFilter, chan<- *block.Block) (subID string, err error)
	Unsubscribe(subID string) error
}

type KeySource interface {
	GetPersistedPrivateKey() (*keys.PrivateKey, error)
	GetPrivateKey() (*keys.PrivateKey, error)
}

type CommonContractPrm struct {
	NEF      nef.File
	Manifest manifest.Manifest
}

type NNSPrm struct {
	CommonContractPrm
	SystemEmail string
}

type AuditPrm struct {
	CommonContractPrm
}

type Prm struct {
	Logger *zap.Logger

	Blockchain Blockchain

	LocalAccount *wallet.Account

	KeySource KeySource

	NNS NNSPrm

	Audit AuditPrm
}

func Deploy(ctx context.Context, prm Prm) error {
	committeeSvc, err := newCommitteeService(prm.Logger, prm.LocalAccount, prm.Blockchain)
	if err != nil {
		return err
	}

	defer committeeSvc.stop()

	syncNNSPrm := syncNNSContractPrm{
		logger:                prm.Logger,
		committeeService:      committeeSvc,
		mustAlreadyBeOnChain:  false,
		localNEF:              prm.NNS.NEF,
		localManifest:         prm.NNS.Manifest,
		systemEmail:           prm.NNS.SystemEmail,
		initCommitteeGroupKey: nil,   // set below
		tryUpdate:             false, // only committee can update the NNS contract, initially this is impossible
	}

	// if local node is the first committee member (Az) => deploy NNS contract,
	// otherwise just wait
	if committeeSvc.isLocalNodeFirstInCommittee() {
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
		syncNNSPrm.initCommitteeGroupKey = keys.NewPrivateKey
	}

	prm.Logger.Info("synchronizing NNS contract with the chain...")

	resSyncNNS, err := syncNNSContract(ctx, syncNNSPrm)
	if err != nil {
		return fmt.Errorf("sync NNS contract with the chain: %w", err)
	}

	prm.Logger.Info("NNS contract successfully synchronized with the chain", zap.Stringer("address", resSyncNNS.onChainAddress))

	prm.Logger.Info("initializing Notary service for the committee...")

	return nil
}
