package engine

import (
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

type ContainerSizePrm struct {
	cid *cid.ID
}

type ContainerSizeRes struct {
	size uint64
}

type ListContainersPrm struct{}

type ListContainersRes struct {
	containers []*cid.ID
}

func (p *ContainerSizePrm) WithContainerID(cid *cid.ID) *ContainerSizePrm {
	if p != nil {
		p.cid = cid
	}

	return p
}

func (r *ContainerSizeRes) Size() uint64 {
	return r.size
}

func (r *ListContainersRes) Containers() []*cid.ID {
	return r.containers
}

// ContainerSize returns sum of estimation container sizes among all shards.
//
// Returns empty result if executions are blocked (see BlockExecution).
func (e *StorageEngine) ContainerSize(prm *ContainerSizePrm) (res *ContainerSizeRes) {
	err := e.exec(func() error {
		res = e.containerSize(prm)
		return nil
	})
	if err != nil {
		e.log.Debug("container size exec failure",
			zap.String("err", err.Error()),
		)
	}

	if res == nil {
		res = new(ContainerSizeRes)
	}

	return
}

// ContainerSize returns sum of estimation container sizes among all shards.
func ContainerSize(e *StorageEngine, id *cid.ID) uint64 {
	return e.ContainerSize(&ContainerSizePrm{cid: id}).Size()
}

func (e *StorageEngine) containerSize(prm *ContainerSizePrm) *ContainerSizeRes {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddEstimateContainerSizeDuration)()
	}

	var res ContainerSizeRes

	e.iterateOverUnsortedShards(func(s *shard.Shard) (stop bool) {
		size, err := shard.ContainerSize(s, prm.cid)
		if err != nil {
			e.log.Warn("can't get container size",
				zap.Stringer("shard_id", s.ID()),
				zap.Stringer("container_id", prm.cid),
				zap.String("error", err.Error()))

			return false
		}

		res.size += size

		return false
	})

	return &res
}

// ListContainers returns unique container IDs presented in the engine objects.
//
// Returns empty result if executions are blocked (see BlockExecution).
func (e *StorageEngine) ListContainers(_ *ListContainersPrm) (res *ListContainersRes) {
	err := e.exec(func() error {
		res = e.listContainers()
		return nil
	})
	if err != nil {
		e.log.Debug("list containers exec failure",
			zap.String("err", err.Error()),
		)
	}

	if res == nil {
		res = new(ListContainersRes)
	}

	return
}

// ListContainers returns unique container IDs presented in the engine objects.
func ListContainers(e *StorageEngine) []*cid.ID {
	return e.ListContainers(&ListContainersPrm{}).Containers()
}

func (e *StorageEngine) listContainers() *ListContainersRes {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListContainersDuration)()
	}

	uniqueIDs := make(map[string]*cid.ID)

	e.iterateOverUnsortedShards(func(s *shard.Shard) (stop bool) {
		cnrs, err := shard.ListContainers(s)
		if err != nil {
			e.log.Warn("can't get list of containers",
				zap.Stringer("shard_id", s.ID()),
				zap.String("error", err.Error()))

			return false
		}

		for i := range cnrs {
			id := cnrs[i].String()
			if _, ok := uniqueIDs[id]; !ok {
				uniqueIDs[id] = cnrs[i]
			}
		}

		return false
	})

	result := make([]*cid.ID, 0, len(uniqueIDs))
	for _, v := range uniqueIDs {
		result = append(result, v)
	}

	return &ListContainersRes{
		containers: result,
	}
}
