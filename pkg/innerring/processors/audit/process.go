package audit

import (
	"context"
	"encoding/hex"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container/wrapper"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

	for i := range containers {
		cnr, err := wrapper.Get(ap.containerClient, containers[i]) // get container structure
		if err != nil {
			log.Error("can't get container info, ignore",
				zap.Stringer("cid", containers[i]),
				zap.String("error", err.Error()))

			continue
		}

		pivot := containers[i].ToV2().GetValue()

		// find all container nodes for current epoch
		nodes, err := nm.GetContainerNodes(cnr.PlacementPolicy(), pivot)
		if err != nil {
			log.Info("can't build placement for container, ignore",
				zap.Stringer("cid", containers[i]),
				zap.String("error", err.Error()))

			continue
		}

		n := nodes.Flatten()
		crand := rand.New() // math/rand with cryptographic source

		// shuffle nodes to ask a random one
		crand.Shuffle(len(n), func(i, j int) {
			n[i], n[j] = n[j], n[i]
		})

		// search storage groups
		storageGroups := ap.findStorageGroups(containers[i], n)
		log.Info("select storage groups for audit",
			zap.Stringer("cid", containers[i]),
			zap.Int("amount", len(storageGroups)))

		// skip audit for containers
		// without storage groups
		if len(storageGroups) == 0 {
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
			WithContainerStructure(cnr).
			WithContainerNodes(nodes).
			WithNetworkMap(nm)

		if err := ap.taskManager.PushTask(auditTask); err != nil {
			ap.log.Error("could not push audit task",
				zap.String("error", err.Error()),
			)
		}
	}
}

func (ap *Processor) findStorageGroups(cid *cid.ID, shuffled netmap.Nodes) []*object.ID {
	var sg []*object.ID

	ln := len(shuffled)

	var (
		info clientcore.NodeInfo
		prm  SearchSGPrm
	)

	prm.id = cid

	for i := range shuffled { // consider iterating over some part of container
		log := ap.log.With(
			zap.Stringer("cid", cid),
			zap.String("key", hex.EncodeToString(shuffled[0].PublicKey())),
			zap.Int("try", i),
			zap.Int("total_tries", ln),
		)

		err := clientcore.NodeInfoFromRawNetmapElement(&info, shuffled[i])
		if err != nil {
			log.Warn("parse client node info", zap.String("error", err.Error()))

			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), ap.searchTimeout)

		prm.ctx = ctx
		prm.info = info

		var dst SearchSGDst

		err = ap.sgSrc.ListSG(&dst, prm)

		cancel()

		if err != nil {
			log.Warn("error in storage group search", zap.String("error", err.Error()))
			continue
		}

		sg = append(sg, dst.ids...)

		break // we found storage groups, so break loop
	}

	return sg
}
