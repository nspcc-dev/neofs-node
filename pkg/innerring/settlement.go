package innerring

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	auditAPI "github.com/nspcc-dev/neofs-api-go/pkg/audit"
	containerAPI "github.com/nspcc-dev/neofs-api-go/pkg/container"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	netmapAPI "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/basic"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	auditClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	balanceClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	containerClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

type globalConfig interface {
	BasicIncomeRate() (uint64, error)
	AuditFee() (uint64, error)
}

type settlementDeps struct {
	globalConfig

	log *logger.Logger

	cnrSrc container.Source

	auditClient *auditClient.ClientWrapper

	nmSrc netmap.Source

	clientCache *ClientCache

	balanceClient *balanceClient.Wrapper
}

type auditSettlementDeps struct {
	*settlementDeps
}

type basicIncomeSettlementDeps struct {
	*settlementDeps
	cnrClient *containerClient.Wrapper
}

type basicSettlementConstructor struct {
	dep *basicIncomeSettlementDeps
}

type auditSettlementCalculator audit.Calculator

type containerWrapper containerAPI.Container

type nodeInfoWrapper struct {
	ni *netmapAPI.Node
}

type sgWrapper storagegroup.StorageGroup

func (s *sgWrapper) Size() uint64 {
	return (*storagegroup.StorageGroup)(s).ValidationDataSize()
}

func (n nodeInfoWrapper) PublicKey() []byte {
	return n.ni.PublicKey()
}

func (n nodeInfoWrapper) Price() *big.Int {
	return big.NewInt(int64(n.ni.Price))
}

func (c *containerWrapper) Owner() *owner.ID {
	return (*containerAPI.Container)(c).OwnerID()
}

func (s settlementDeps) AuditResultsForEpoch(epoch uint64) ([]*auditAPI.Result, error) {
	idList, err := s.auditClient.ListAuditResultIDByEpoch(epoch)
	if err != nil {
		return nil, fmt.Errorf("could not list audit results in sidechain: %w", err)
	}

	res := make([]*auditAPI.Result, 0, len(idList))

	for i := range idList {
		r, err := s.auditClient.GetAuditResult(idList[i])
		if err != nil {
			return nil, fmt.Errorf("could not get audit result: %w", err)
		}

		res = append(res, r)
	}

	return res, nil
}

func (s settlementDeps) ContainerInfo(cid *cid.ID) (common.ContainerInfo, error) {
	cnr, err := s.cnrSrc.Get(cid)
	if err != nil {
		return nil, fmt.Errorf("could not get container from storage: %w", err)
	}

	return (*containerWrapper)(cnr), nil
}

func (s settlementDeps) buildContainer(e uint64, cid *cid.ID) (netmapAPI.ContainerNodes, *netmapAPI.Netmap, error) {
	var (
		nm  *netmapAPI.Netmap
		err error
	)

	if e > 0 {
		nm, err = s.nmSrc.GetNetMapByEpoch(e)
	} else {
		nm, err = netmap.GetLatestNetworkMap(s.nmSrc)
	}

	if err != nil {
		return nil, nil, fmt.Errorf("could not get network map from storage: %w", err)
	}

	cnr, err := s.cnrSrc.Get(cid)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get container from sidechain: %w", err)
	}

	cn, err := nm.GetContainerNodes(
		cnr.PlacementPolicy(),
		cid.ToV2().GetValue(), // may be replace pivot calculation to neofs-api-go
	)
	if err != nil {
		return nil, nil, fmt.Errorf("could not calculate container nodes: %w", err)
	}

	return cn, nm, nil
}

func (s settlementDeps) ContainerNodes(e uint64, cid *cid.ID) ([]common.NodeInfo, error) {
	cn, _, err := s.buildContainer(e, cid)
	if err != nil {
		return nil, err
	}

	ns := cn.Flatten()
	res := make([]common.NodeInfo, 0, len(ns))

	for i := range ns {
		res = append(res, &nodeInfoWrapper{
			ni: ns[i],
		})
	}

	return res, nil
}

func (s settlementDeps) SGInfo(addr *object.Address) (audit.SGInfo, error) {
	cn, nm, err := s.buildContainer(0, addr.ContainerID())
	if err != nil {
		return nil, err
	}

	sg, err := s.clientCache.getSG(context.Background(), addr, nm, cn)
	if err != nil {
		return nil, err
	}

	return (*sgWrapper)(sg), nil
}

func (s settlementDeps) ResolveKey(ni common.NodeInfo) (*owner.ID, error) {
	pub, err := keys.NewPublicKeyFromBytes(ni.PublicKey(), elliptic.P256())
	if err != nil {
		return nil, err
	}

	w, err := owner.NEO3WalletFromPublicKey((*ecdsa.PublicKey)(pub))
	if err != nil {
		return nil, err
	}

	id := owner.NewID()
	id.SetNeo3Wallet(w)

	return id, nil
}

func (s settlementDeps) Transfer(sender, recipient *owner.ID, amount *big.Int, details []byte) {
	log := s.log.With(
		zap.Stringer("sender", sender),
		zap.Stringer("recipient", recipient),
		zap.Stringer("amount (GASe-12)", amount),
		zap.String("details", hex.EncodeToString(details)),
	)

	if !amount.IsInt64() {
		s.log.Error("amount can not be represented as an int64")

		return
	}

	params := balanceClient.TransferPrm{
		Amount:  amount.Int64(),
		From:    sender,
		To:      recipient,
		Details: details,
	}

	err := s.balanceClient.TransferX(params)
	if err != nil {
		log.Error("could not send transfer transaction for audit",
			zap.String("error", err.Error()),
		)

		return
	}

	log.Debug("transfer transaction for audit was successfully sent")
}

func (b basicIncomeSettlementDeps) BasicRate() (uint64, error) {
	return b.BasicIncomeRate()
}

func (b basicIncomeSettlementDeps) Estimations(epoch uint64) ([]*wrapper.Estimations, error) {
	estimationIDs, err := b.cnrClient.ListLoadEstimationsByEpoch(epoch)
	if err != nil {
		return nil, err
	}

	result := make([]*wrapper.Estimations, 0, len(estimationIDs))

	for i := range estimationIDs {
		estimation, err := b.cnrClient.GetUsedSpaceEstimations(estimationIDs[i])
		if err != nil {
			b.log.Warn("can't get used space estimation",
				zap.String("estimation_id", hex.EncodeToString(estimationIDs[i])),
				zap.String("error", err.Error()))

			continue
		}

		result = append(result, estimation)
	}

	return result, nil
}

func (b basicIncomeSettlementDeps) Balance(id *owner.ID) (*big.Int, error) {
	return b.balanceClient.BalanceOf(id)
}

func (s *auditSettlementCalculator) ProcessAuditSettlements(epoch uint64) {
	(*audit.Calculator)(s).Calculate(&audit.CalculatePrm{
		Epoch: epoch,
	})
}

func (b *basicSettlementConstructor) CreateContext(epoch uint64) (*basic.IncomeSettlementContext, error) {
	return basic.NewIncomeSettlementContext(&basic.IncomeSettlementContextPrms{
		Log:         b.dep.log,
		Epoch:       epoch,
		Rate:        b.dep,
		Estimations: b.dep,
		Balances:    b.dep,
		Container:   b.dep,
		Placement:   b.dep,
		Exchange:    b.dep,
		Accounts:    b.dep,
	})
}
