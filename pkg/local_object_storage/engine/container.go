package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
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
func (e *StorageEngine) ContainerSize(prm *ContainerSizePrm) *ContainerSizeRes {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddEstimateContainerSizeDuration)()
	}

	return &ContainerSizeRes{
		size: e.containerSize(prm.cid),
	}
}

// ContainerSize returns sum of estimation container sizes among all shards.
func ContainerSize(e *StorageEngine, id *cid.ID) uint64 {
	return e.ContainerSize(&ContainerSizePrm{cid: id}).Size()
}

func (e *StorageEngine) containerSize(id *cid.ID) (total uint64) {
	e.iterateOverUnsortedShards(func(s *shard.Shard) (stop bool) {
		size, err := shard.ContainerSize(s, id)
		if err != nil {
			e.log.Warn("can't get container size",
				zap.Stringer("shard_id", s.ID()),
				zap.Stringer("container_id", id),
				zap.String("error", err.Error()))

			return false
		}

		total += size

		return false
	})

	return total
}

// ListContainers returns unique container IDs presented in the engine objects.
func (e *StorageEngine) ListContainers(_ *ListContainersPrm) *ListContainersRes {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListContainersDuration)()
	}

	return &ListContainersRes{
		containers: e.listContainers(),
	}
}

// ListContainers returns unique container IDs presented in the engine objects.
func ListContainers(e *StorageEngine) []*cid.ID {
	return e.ListContainers(&ListContainersPrm{}).Containers()
}

func (e *StorageEngine) listContainers() []*cid.ID {
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

	return result
}
