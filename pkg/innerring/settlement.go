package innerring

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/basic"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/common"
	auditClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit"
	balanceClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	containerClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	netmapClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	auditsvc "github.com/nspcc-dev/neofs-node/pkg/services/audit"
	containerAPI "github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapAPI "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/storagegroup"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

const (
	auditSettlementContext       = "audit"
	basicIncomeSettlementContext = "basic income"
)

type settlementDeps struct {
	log *zap.Logger

	cnrSrc container.Source

	auditClient *auditClient.Client

	nmClient *netmapClient.Client

	clientCache *ClientCache

	balanceClient *balanceClient.Client

	settlementCtx string
}

type auditSettlementDeps struct {
	settlementDeps
}

type basicIncomeSettlementDeps struct {
	settlementDeps
	cnrClient *containerClient.Client
}

type basicSettlementConstructor struct {
	dep *basicIncomeSettlementDeps
}

type auditSettlementCalculator audit.Calculator

type containerWrapper containerAPI.Container

type nodeInfoWrapper struct {
	ni netmapAPI.NodeInfo
}

type sgWrapper storagegroup.StorageGroup

func (s *sgWrapper) Size() uint64 {
	return (*storagegroup.StorageGroup)(s).ValidationDataSize()
}

func (n nodeInfoWrapper) PublicKey() []byte {
	return n.ni.PublicKey()
}

func (n nodeInfoWrapper) Price() *big.Int {
	return big.NewInt(int64(n.ni.Price()))
}

func (c containerWrapper) Owner() user.ID {
	return (containerAPI.Container)(c).Owner()
}

func (s settlementDeps) AuditResultsForEpoch(epoch uint64) ([]*auditsvc.Result, error) {
	idList, err := s.auditClient.ListAuditResultIDByEpoch(epoch)
	if err != nil {
		return nil, fmt.Errorf("could not list audit results in FS chain: %w", err)
	}

	res := make([]*auditsvc.Result, 0, len(idList))

	for i := range idList {
		r, err := s.auditClient.GetAuditResult(idList[i])
		if err != nil {
			return nil, fmt.Errorf("could not get audit result: %w", err)
		}

		res = append(res, r)
	}

	return res, nil
}

func (s settlementDeps) ContainerInfo(cid cid.ID) (common.ContainerInfo, error) {
	cnr, err := s.cnrSrc.Get(cid)
	if err != nil {
		return nil, fmt.Errorf("could not get container from storage: %w", err)
	}

	return (containerWrapper)(cnr.Value), nil
}

func (s settlementDeps) buildContainer(e uint64, cid cid.ID) ([][]netmapAPI.NodeInfo, *netmapAPI.NetMap, error) {
	var (
		nm  *netmapAPI.NetMap
		err error
	)

	if e > 0 {
		nm, err = s.nmClient.GetNetMapByEpoch(e)
	} else {
		nm, err = s.nmClient.NetMap()
	}

	if err != nil {
		return nil, nil, fmt.Errorf("could not get network map from storage: %w", err)
	}

	cnr, err := s.cnrSrc.Get(cid)
	if err != nil {
		return nil, nil, fmt.Errorf("could not get container from FS chain: %w", err)
	}

	cn, err := nm.ContainerNodes(
		cnr.Value.PlacementPolicy(),
		cid,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("could not calculate container nodes: %w", err)
	}

	return cn, nm, nil
}

func (s settlementDeps) ContainerNodes(e uint64, cid cid.ID) ([]common.NodeInfo, error) {
	cn, _, err := s.buildContainer(e, cid)
	if err != nil {
		return nil, err
	}

	var sz int

	for i := range cn {
		sz += len(cn[i])
	}

	res := make([]common.NodeInfo, 0, sz)

	for i := range cn {
		for j := range cn[i] {
			res = append(res, nodeInfoWrapper{
				ni: cn[i][j],
			})
		}
	}

	return res, nil
}

// SGInfo returns audit.SGInfo by object address.
//
// Returns an error of type apistatus.ObjectNotFound if storage group is missing.
func (s settlementDeps) SGInfo(addr oid.Address) (audit.SGInfo, error) {
	cnr := addr.Container()

	cn, nm, err := s.buildContainer(0, cnr)
	if err != nil {
		return nil, err
	}

	sg, err := s.clientCache.getSG(context.Background(), addr, nm, cn)
	if err != nil {
		return nil, err
	}

	return (*sgWrapper)(sg), nil
}

func (s settlementDeps) ResolveKey(ni common.NodeInfo) (*user.ID, error) {
	pubKey, err := keys.NewPublicKeyFromBytes(ni.PublicKey(), elliptic.P256())
	if err != nil {
		return nil, fmt.Errorf("decode public key: %w", err)
	}

	id := user.NewFromECDSAPublicKey(ecdsa.PublicKey(*pubKey))

	return &id, nil
}

func (s settlementDeps) Transfer(sender, recipient user.ID, amount *big.Int, details []byte) {
	if s.settlementCtx == "" {
		panic("unknown settlement deps context")
	}

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
		log.Error(fmt.Sprintf("%s: could not send transfer", s.settlementCtx),
			zap.Error(err),
		)

		return
	}

	log.Debug(fmt.Sprintf("%s: transfer was successfully sent", s.settlementCtx))
}

func (b basicIncomeSettlementDeps) BasicRate() (uint64, error) {
	return b.nmClient.BasicIncomeRate()
}

func (b basicIncomeSettlementDeps) Estimations(epoch uint64) (map[cid.ID]*containerClient.Estimations, error) {
	return b.cnrClient.ListLoadEstimationsByEpoch(epoch)
}

func (b basicIncomeSettlementDeps) Balance(id user.ID) (*big.Int, error) {
	return b.balanceClient.BalanceOf(id)
}

func (s *auditSettlementCalculator) ProcessAuditSettlements(epoch uint64) {
	(*audit.Calculator)(s).Calculate(epoch)
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
	}), nil
}
