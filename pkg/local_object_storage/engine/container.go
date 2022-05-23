package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
)

// ContainerSizePrm groups parameters of ContainerSize operation.
type ContainerSizePrm struct {
	cnr cid.ID
}

// ContainerSizeRes resulting values of ContainerSize operation.
type ContainerSizeRes struct {
	size uint64
}

// ListContainersPrm groups parameters of ListContainers operation.
type ListContainersPrm struct{}

// ListContainersRes groups the resulting values of ListContainers operation.
type ListContainersRes struct {
	containers []cid.ID
}

// SetContainerID sets the identifier of the container to estimate the size.
func (p *ContainerSizePrm) SetContainerID(cnr cid.ID) {
	p.cnr = cnr
}

// Size returns calculated estimation of the container size.
func (r ContainerSizeRes) Size() uint64 {
	return r.size
}

// Containers returns a list of identifiers of the containers in which local objects are stored.
func (r ListContainersRes) Containers() []cid.ID {
	return r.containers
}

// ContainerSize returns the sum of estimation container sizes among all shards.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) ContainerSize(prm ContainerSizePrm) (res *ContainerSizeRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.containerSize(prm)
		return err
	})

	return
}

// ContainerSize calls ContainerSize method on engine to calculate sum of estimation container sizes among all shards.
func ContainerSize(e *StorageEngine, id cid.ID) (uint64, error) {
	var prm ContainerSizePrm

	prm.SetContainerID(id)

	res, err := e.ContainerSize(prm)
	if err != nil {
		return 0, err
	}

	return res.Size(), nil
}

func (e *StorageEngine) containerSize(prm ContainerSizePrm) (*ContainerSizeRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddEstimateContainerSizeDuration)()
	}

	res := new(ContainerSizeRes)

	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		size, err := shard.ContainerSize(sh.Shard, prm.cnr)
		if err != nil {
			e.reportShardError(sh, "can't get container size", err,
				zap.Stringer("container_id", prm.cnr),
			)
			return false
		}

		res.size += size

		return false
	})

	return res, nil
}

// ListContainers returns a unique container IDs presented in the engine objects.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) ListContainers(_ ListContainersPrm) (res *ListContainersRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.listContainers()
		return err
	})

	return
}

// ListContainers calls ListContainers method on engine to get a unique container IDs presented in the engine objects.
func ListContainers(e *StorageEngine) ([]cid.ID, error) {
	var prm ListContainersPrm

	res, err := e.ListContainers(prm)
	if err != nil {
		return nil, err
	}

	return res.Containers(), nil
}

func (e *StorageEngine) listContainers() (*ListContainersRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListContainersDuration)()
	}

	uniqueIDs := make(map[string]cid.ID)

	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		cnrs, err := shard.ListContainers(sh.Shard)
		if err != nil {
			e.reportShardError(sh, "can't get list of containers", err)
			return false
		}

		for i := range cnrs {
			id := cnrs[i].EncodeToString()
			if _, ok := uniqueIDs[id]; !ok {
				uniqueIDs[id] = cnrs[i]
			}
		}

		return false
	})

	result := make([]cid.ID, 0, len(uniqueIDs))
	for _, v := range uniqueIDs {
		result = append(result, v)
	}

	return &ListContainersRes{
		containers: result,
	}, nil
}
