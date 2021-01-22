package engine

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

type ContainerSizePrm struct {
	cid *container.ID
}

type ContainerSizeRes struct {
	size uint64
}

func (p *ContainerSizePrm) WithContainerID(cid *container.ID) *ContainerSizePrm {
	if p != nil {
		p.cid = cid
	}

	return p
}

func (r *ContainerSizeRes) Size() uint64 {
	return r.size
}

// ContainerSize returns sum of estimation container sizes among all shards.
func (e *StorageEngine) ContainerSize(prm *ContainerSizePrm) *ContainerSizeRes {
	return &ContainerSizeRes{
		size: e.containerSize(prm.cid),
	}
}

// ContainerSize returns sum of estimation container sizes among all shards.
func ContainerSize(e *StorageEngine, id *container.ID) uint64 {
	return e.ContainerSize(&ContainerSizePrm{cid: id}).Size()
}

func (e *StorageEngine) containerSize(id *container.ID) (total uint64) {
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
