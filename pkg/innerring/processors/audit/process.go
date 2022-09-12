package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/storagegroup"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

func (ap *Processor) processStartAudit(epoch uint64) {
	log := ap.log.With(zap.Uint64("epoch", epoch))

	ap.prevAuditCanceler()

	skipped := ap.taskManager.Reset()
	if skipped > 0 {
		ap.log.Info("some tasks from previous epoch are skipped",
			zap.Int("amount", skipped),
		)
	}

	containers, err := ap.selectContainersToAudit(epoch)
	if err != nil {
		log.Error("container selection failure", zap.String("error", err.Error()))

		return
	}

	log.Info("select containers for audit", zap.Int("amount", len(containers)))

	nm, err := ap.netmapClient.GetNetMap(0)
	if err != nil {
		ap.log.Error("can't fetch network map",
			zap.String("error", err.Error()))

		return
	}

	var auditCtx context.Context
	auditCtx, ap.prevAuditCanceler = context.WithCancel(context.Background())

	pivot := make([]byte, sha256.Size)

	for i := range containers {
		cnr, err := cntClient.Get(ap.containerClient, containers[i]) // get container structure
		if err != nil {
			log.Error("can't get container info, ignore",
				zap.Stringer("cid", containers[i]),
				zap.String("error", err.Error()))

			continue
		}

		containers[i].Encode(pivot)

		// find all container nodes for current epoch
		nodes, err := nm.ContainerNodes(cnr.Value.PlacementPolicy(), pivot)
		if err != nil {
			log.Info("can't build placement for container, ignore",
				zap.Stringer("cid", containers[i]),
				zap.String("error", err.Error()))

			continue
		}

		n := placement.FlattenNodes(nodes)

		// shuffle nodes to ask a random one
		rand.Shuffle(len(n), func(i, j int) {
			n[i], n[j] = n[j], n[i]
		})

		// search storage groups
		storageGroupsIDs := ap.findStorageGroups(containers[i], n)
		log.Info("select storage groups for audit",
			zap.Stringer("cid", containers[i]),
			zap.Int("amount", len(storageGroupsIDs)))

		// filter expired storage groups
		storageGroups := ap.filterExpiredSG(containers[i], storageGroupsIDs, nodes, *nm)
		log.Info("filter expired storage groups for audit",
			zap.Stringer("cid", containers[i]),
			zap.Int("amount", len(storageGroups)))

		// skip audit for containers without
		// non-expired storage groups
		if len(storageGroupsIDs) == 0 {
			continue
		}

		auditTask := new(audit.Task).
			WithReporter(&epochAuditReporter{
				epoch: epoch,
				rep:   ap.reporter,
			}).
			WithAuditContext(auditCtx).
			WithContainerID(containers[i]).
			WithStorageGroupList(storageGroups).
			WithContainerStructure(cnr.Value).
			WithContainerNodes(nodes).
			WithNetworkMap(nm)

		if err := ap.taskManager.PushTask(auditTask); err != nil {
			ap.log.Error("could not push audit task",
				zap.String("error", err.Error()),
			)
		}
	}
}

func (ap *Processor) findStorageGroups(cnr cid.ID, shuffled netmapcore.Nodes) []oid.ID {
	var sg []oid.ID

	ln := len(shuffled)

	var (
		info clientcore.NodeInfo
		prm  storagegroup.SearchSGPrm
	)

	prm.Container = cnr

	for i := range shuffled { // consider iterating over some part of container
		log := ap.log.With(
			zap.Stringer("cid", cnr),
			zap.String("key", hex.EncodeToString(shuffled[0].PublicKey())),
			zap.Int("try", i),
			zap.Int("total_tries", ln),
		)

		err := clientcore.NodeInfoFromRawNetmapElement(&info, netmapcore.Node(shuffled[i]))
		if err != nil {
			log.Warn("parse client node info", zap.String("error", err.Error()))

			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), ap.searchTimeout)

		prm.Context = ctx
		prm.NodeInfo = info

		var dst storagegroup.SearchSGDst

		err = ap.sgSrc.ListSG(&dst, prm)

		cancel()

		if err != nil {
			log.Warn("error in storage group search", zap.String("error", err.Error()))
			continue
		}

		sg = append(sg, dst.Objects...)

		break // we found storage groups, so break loop
	}

	return sg
}

func (ap *Processor) filterExpiredSG(cid cid.ID, sgIDs []oid.ID,
	cnr [][]netmap.NodeInfo, nm netmap.NetMap) []storagegroup.StorageGroup {
	sgs := make([]storagegroup.StorageGroup, 0, len(sgIDs))
	var coreSG storagegroup.StorageGroup

	var getSGPrm storagegroup.GetSGPrm
	getSGPrm.CID = cid
	getSGPrm.Container = cnr
	getSGPrm.NetMap = nm

	for _, sgID := range sgIDs {
		ctx, cancel := context.WithTimeout(context.Background(), ap.searchTimeout)

		getSGPrm.OID = sgID
		getSGPrm.Context = ctx

		sg, err := ap.sgSrc.GetSG(getSGPrm)

		cancel()

		if err != nil {
			ap.log.Error(
				"could not get storage group object for audit, skipping",
				zap.Stringer("cid", cid),
				zap.Stringer("oid", sgID),
				zap.Error(err),
			)
			continue
		}

		// filter expired epochs
		if sg.ExpirationEpoch() >= ap.epochSrc.EpochCounter() {
			coreSG.SetID(sgID)
			coreSG.SetStorageGroup(*sg)

			sgs = append(sgs, coreSG)
		}
	}

	return sgs
}
