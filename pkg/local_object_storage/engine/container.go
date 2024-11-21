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

// ContainerSize returns the sum of estimation container sizes among all shards.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) ContainerSize(cnr cid.ID) (uint64, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddEstimateContainerSizeDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return 0, e.blockErr
	}

	var size uint64

	for _, sh := range e.unsortedShards() {
		shardSize, err := sh.Shard.ContainerSize(cnr)
		if err != nil {
			e.reportShardError(sh, "can't get container size", err,
				zap.Stringer("container_id", cnr))
			continue
		}

		size += shardSize
	}

	return size, nil
}

// ListContainers returns a unique container IDs presented in the engine objects.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) ListContainers() ([]cid.ID, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListContainersDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	uniqueIDs := make(map[cid.ID]struct{})

	for _, sh := range e.unsortedShards() {
		res, err := sh.Shard.ListContainers(shard.ListContainersPrm{})
		if err != nil {
			e.reportShardError(sh, "can't get list of containers", err)
			continue
		}

		for _, cnr := range res.Containers() {
			if _, ok := uniqueIDs[cnr]; !ok {
				uniqueIDs[cnr] = struct{}{}
			}
		}
	}

	result := make([]cid.ID, 0, len(uniqueIDs))
	for cnr := range uniqueIDs {
		result = append(result, cnr)
	}

	return result, nil
}

// DeleteContainer deletes container's objects that engine stores.
func (e *StorageEngine) DeleteContainer(ctx context.Context, cID cid.ID) error {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return e.blockErr
	}

	var wg errgroup.Group

	for _, sh := range e.unsortedShards() {
		wg.Go(func() error {
			err := sh.Shard.DeleteContainer(ctx, cID)
			if err != nil {
				err = fmt.Errorf("container cleanup in %s shard: %w", sh.ID(), err)
				e.log.Warn("container cleanup", zap.Error(err))

				return err
			}

			return nil
		})
	}

	return wg.Wait()
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
