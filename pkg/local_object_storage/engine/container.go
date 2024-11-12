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
	var (
		err  error
		size uint64
	)
	if e.metrics != nil {
		defer elapsed(e.metrics.AddEstimateContainerSizeDuration)()
	}

	err = e.execIfNotBlocked(func() error {
		size, err = e.containerSize(cnr)
		return err
	})

	return size, err
}

func (e *StorageEngine) containerSize(cnr cid.ID) (uint64, error) {
	var size uint64

	e.iterateOverUnsortedShards(func(sh hashedShard) (stop bool) {
		var csPrm shard.ContainerSizePrm
		csPrm.SetContainerID(cnr)

		csRes, err := sh.Shard.ContainerSize(csPrm)
		if err != nil {
			e.reportShardError(sh, "can't get container size", err,
				zap.Stringer("container_id", cnr))
			return false
		}

		size += csRes.Size()

		return false
	})

	return size, nil
}

// ListContainers returns a unique container IDs presented in the engine objects.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) ListContainers() ([]cid.ID, error) {
	var (
		res []cid.ID
		err error
	)
	if e.metrics != nil {
		defer elapsed(e.metrics.AddListContainersDuration)()
	}

	err = e.execIfNotBlocked(func() error {
		res, err = e.listContainers()
		return err
	})

	return res, err
}

func (e *StorageEngine) listContainers() ([]cid.ID, error) {
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

	return result, nil
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
