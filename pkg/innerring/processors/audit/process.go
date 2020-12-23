package audit

import (
	"context"
	"math/rand"
	"time"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/audit"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/storagegroup"
	"go.uber.org/zap"
)

var sgFilter = storagegroup.SearchQuery()

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

	for i := range containers {
		cnr, err := ap.containerClient.Get(containers[i]) // get container structure
		if err != nil {
			log.Error("can't get container info, ignore",
				zap.Stringer("cid", containers[i]),
				zap.String("error", err.Error()))

			continue
		}

		// find all container nodes for current epoch
		nodes, err := nm.GetContainerNodes(cnr.PlacementPolicy(), nil)
		if err != nil {
			log.Info("can't build placement for container, ignore",
				zap.Stringer("cid", containers[i]),
				zap.String("error", err.Error()))

			continue
		}

		// shuffle nodes to ask a random one
		n := nodes.Flatten()
		rand.Shuffle(len(n), func(i, j int) { // fixme: consider using crypto rand
			n[i], n[j] = n[j], n[i]
		})

		// search storage groups
		storageGroups := ap.findStorageGroups(containers[i], n)
		log.Info("select storage groups for audit",
			zap.Stringer("cid", containers[i]),
			zap.Int("amount", len(storageGroups)))

		var auditCtx context.Context
		auditCtx, ap.prevAuditCanceler = context.WithCancel(context.Background())

		auditTask := new(audit.Task).
			WithReporter(&epochAuditReporter{
				epoch: epoch,
				rep:   ap.reporter,
			}).
			WithAuditContext(auditCtx).
			WithContainerID(containers[i]).
			WithStorageGroupList(storageGroups).
			WithContainerStructure(cnr).
			WithContainerNodes(nodes)

		if err := ap.taskManager.PushTask(auditTask); err != nil {
			ap.log.Error("could not push audit task",
				zap.String("error", err.Error()),
			)
		}
	}
}

func (ap *Processor) findStorageGroups(cid *container.ID, shuffled netmap.Nodes) []*object.ID {
	var sg []*object.ID

	ln := len(shuffled)

	for i := range shuffled { // consider iterating over some part of container
		log := ap.log.With(
			zap.Stringer("cid", cid),
			zap.String("address", shuffled[0].Address()),
			zap.Int("try", i),
			zap.Int("total_tries", ln),
		)

		addr, err := network.IPAddrFromMultiaddr(shuffled[i].Address())
		if err != nil {
			log.Warn("can't parse remote address", zap.String("error", err.Error()))

			continue
		}

		cli, err := ap.clientCache.Get(addr)
		if err != nil {
			log.Warn("can't setup remote connection", zap.String("error", err.Error()))

			continue
		}

		sgSearchParams := &client.SearchObjectParams{}
		sgSearchParams.WithContainerID(cid)
		sgSearchParams.WithSearchFilters(sgFilter)

		// fixme: timeout from config
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := cli.SearchObject(ctx, sgSearchParams)
		cancel()

		if err != nil {
			log.Warn("error in storage group search", zap.String("error", err.Error()))
			continue
		}

		sg = append(sg, result...)

		break // we found storage groups, so break loop
	}

	return sg
}
