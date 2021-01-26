package innerring

import (
	"context"
	"math/big"

	auditAPI "github.com/nspcc-dev/neofs-api-go/pkg/audit"
	containerAPI "github.com/nspcc-dev/neofs-api-go/pkg/container"
	netmapAPI "github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-api-go/pkg/storagegroup"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement/audit"
	auditClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/audit/wrapper"
	balanceClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/balance/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type auditSettlementDeps struct {
	log *logger.Logger

	cnrSrc container.Source

	auditClient *auditClient.ClientWrapper

	nmSrc netmap.Source

	clientCache *ClientCache

	balanceClient *balanceClient.Wrapper
}

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

func (a auditSettlementDeps) AuditResultsForEpoch(epoch uint64) ([]*auditAPI.Result, error) {
	idList, err := a.auditClient.ListAuditResultIDByEpoch(epoch)
	if err != nil {
		return nil, errors.Wrap(err, "could not list audit results in sidechain")
	}

	res := make([]*auditAPI.Result, 0, len(idList))

	for i := range idList {
		r, err := a.auditClient.GetAuditResult(idList[i])
		if err != nil {
			return nil, errors.Wrap(err, "could not get audit result")
		}

		res = append(res, r)
	}

	return res, nil
}

func (a auditSettlementDeps) ContainerInfo(cid *containerAPI.ID) (audit.ContainerInfo, error) {
	cnr, err := a.cnrSrc.Get(cid)
	if err != nil {
		return nil, errors.Wrap(err, "could not get container from storage")
	}

	return (*containerWrapper)(cnr), nil
}

func (a auditSettlementDeps) buildContainer(e uint64, cid *containerAPI.ID) (netmapAPI.ContainerNodes, *netmapAPI.Netmap, error) {
	var (
		nm  *netmapAPI.Netmap
		err error
	)

	if e > 0 {
		nm, err = a.nmSrc.GetNetMapByEpoch(e)
	} else {
		nm, err = netmap.GetLatestNetworkMap(a.nmSrc)
	}

	if err != nil {
		return nil, nil, errors.Wrap(err, "could not get network map from storage")
	}

	cnr, err := a.cnrSrc.Get(cid)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not get container from sidechain")
	}

	cn, err := nm.GetContainerNodes(
		cnr.PlacementPolicy(),
		cid.ToV2().GetValue(), // may be replace pivot calculation to neofs-api-go
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not calculate container nodes")
	}

	return cn, nm, nil
}

func (a auditSettlementDeps) ContainerNodes(e uint64, cid *containerAPI.ID) ([]audit.NodeInfo, error) {
	cn, _, err := a.buildContainer(e, cid)
	if err != nil {
		return nil, err
	}

	ns := cn.Flatten()
	res := make([]audit.NodeInfo, 0, len(ns))

	for i := range ns {
		res = append(res, &nodeInfoWrapper{
			ni: ns[i],
		})
	}

	return res, nil
}

func (a auditSettlementDeps) SGInfo(addr *object.Address) (audit.SGInfo, error) {
	cn, nm, err := a.buildContainer(0, addr.ContainerID())
	if err != nil {
		return nil, err
	}

	sg, err := a.clientCache.getSG(context.Background(), addr, nm, cn)
	if err != nil {
		return nil, err
	}

	return (*sgWrapper)(sg), nil
}

func (a auditSettlementDeps) ResolveKey(ni audit.NodeInfo) (*owner.ID, error) {
	w, err := owner.NEO3WalletFromPublicKey(crypto.UnmarshalPublicKey(ni.PublicKey()))
	if err != nil {
		return nil, err
	}

	id := owner.NewID()
	id.SetNeo3Wallet(w)

	return id, nil
}

var transferAuditDetails = []byte("settlement-audit")

func (a auditSettlementDeps) Transfer(sender, recipient *owner.ID, amount *big.Int) {
	if !amount.IsInt64() {
		a.log.Error("amount can not be represented as an int64",
			zap.Stringer("value", amount),
		)

		return
	}

	amount64 := amount.Int64()
	if amount64 == 0 {
		amount64 = 1
	}

	if err := a.balanceClient.TransferX(balanceClient.TransferPrm{
		Amount:  amount64,
		From:    sender,
		To:      recipient,
		Details: transferAuditDetails,
	}); err != nil {
		a.log.Error("transfer of funds for audit failed",
			zap.String("error", err.Error()),
		)
	}
}
