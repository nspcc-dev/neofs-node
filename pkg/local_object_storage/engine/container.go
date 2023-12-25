package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
func (e *StorageEngine) ContainerSize(prm ContainerSizePrm) (res ContainerSizeRes, err error) {
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

func (e *StorageEngine) containerSize(prm ContainerSizePrm) (res ContainerSizeRes, err error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddEstimateContainerSizeDuration)()
	}

	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		var csPrm shard.ContainerSizePrm
		csPrm.SetContainerID(prm.cnr)

		csRes, err := sh.Shard.ContainerSize(csPrm)
		if err != nil {
			e.reportShardError(sh, "can't get container size", err,
				zap.Stringer("container_id", prm.cnr))
			return false
		}

		res.size += csRes.Size()

		return false
	})

	return
}

// ListContainers returns a unique container IDs presented in the engine objects.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) ListContainers(_ ListContainersPrm) (res ListContainersRes, err error) {
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

func (e *StorageEngine) listContainers() (ListContainersRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListContainersDuration)()
	}

	uniqueIDs := make(map[cid.ID]struct{})

	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		res, err := sh.Shard.ListContainers(shard.ListContainersPrm{})
		if err != nil {
			e.reportShardError(sh, "can't get list of containers", err)
			return false
		}

		for _, cnr := range res.Containers() {
			if _, ok := uniqueIDs[cnr]; !ok {
				uniqueIDs[cnr] = struct{}{}
			}
		}

		return false
	})

	result := make([]cid.ID, 0, len(uniqueIDs))
	for cnr := range uniqueIDs {
		result = append(result, cnr)
	}

	return ListContainersRes{
		containers: result,
	}, nil
}

// DeleteContainer deletes container's objects that engine stores.
func (e *StorageEngine) DeleteContainer(ctx context.Context, cID cid.ID) error {
	return e.execIfNotBlocked(func() error {
		var wg errgroup.Group

		e.iterateOverUnsortedShards(func(hs hashedShard) bool {
			wg.Go(func() error {
				err := hs.Shard.DeleteContainer(ctx, cID)
				if err != nil {
					err = fmt.Errorf("container cleanup in %s shard: %w", hs.ID(), err)
					e.log.Warn("container cleanup", zap.Error(err))

					return err
				}

				return nil
			})

			return false
		})

		return wg.Wait()
	})
}

func (e *StorageEngine) deleteNotFoundContainers() error {
	if e.cfg.containerSource == nil {
		return nil
	}

	var wg errgroup.Group
	for i := range e.shards {
		iCopy := i

		wg.Go(func() error {
			shID := e.shards[iCopy].ID()

			res, err := e.shards[iCopy].ListContainers(shard.ListContainersPrm{})
			if err != nil {
				return fmt.Errorf("fetching containers from '%s' shard: %w", shID, err)
			}

			for _, cnrStored := range res.Containers() {
				// in the most loaded scenarios it is a cache
				if _, err = e.cfg.containerSource.Get(cnrStored); errors.As(err, new(apistatus.ContainerNotFound)) {
					err = e.shards[iCopy].InhumeContainer(cnrStored)
					if err != nil {
						return fmt.Errorf("'%s' container cleanup in '%s' shard: %w", cnrStored, shID, err)
					}
				}
			}

			return nil
		})
	}

	return wg.Wait()
}
