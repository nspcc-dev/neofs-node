package engine

import (
	"errors"
	"sync"

	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const (
	existsParallelThreshold = 4
)

type shardExistsResult struct {
	exists bool
	err    error
}

func (e *StorageEngine) existsPhysical(addr oid.Address) (bool, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddExistsDuration)()
	}

	shs := e.sortedShards(addr.Object())
	if len(shs) < existsParallelThreshold {
		return e.existsPhysicalSequential(addr, shs)
	}
	return e.existsPhysicalParallel(addr, shs)
}

func (e *StorageEngine) existsPhysicalSequential(addr oid.Address, shs []shardWrapper) (bool, error) {
	for _, sh := range shs {
		exists, err := sh.Exists(addr, false)
		if err != nil {
			if shard.IsErrObjectExpired(err) {
				return true, nil
			}

			if isObjectPresenceStatus(err) {
				return false, err
			}

			e.reportShardError(sh, "could not check existence of object in shard", err)
			continue
		}

		if exists {
			return true, nil
		}
	}

	return false, nil
}

func (e *StorageEngine) existsPhysicalParallel(addr oid.Address, shs []shardWrapper) (bool, error) {
	results := make([]shardExistsResult, len(shs))
	workers := (len(shs) + listParallelThreshold - 1) / listParallelThreshold
	chunkSize := (len(shs) + workers - 1) / workers

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		start := w * chunkSize
		end := start + chunkSize
		end = min(end, len(shs))

		go func(start, end int) {
			defer wg.Done()
			for i := start; i < end; i++ {
				results[i].exists, results[i].err = shs[i].Exists(addr, false)
			}
		}(start, end)
	}
	wg.Wait()

	for i, res := range results {
		if res.err != nil {
			if shard.IsErrObjectExpired(res.err) {
				return true, nil
			}
			if isObjectPresenceStatus(res.err) {
				return false, res.err
			}
			e.reportShardError(shs[i], "could not check existence of object in shard", res.err)
			continue
		}
		if res.exists {
			return true, nil
		}
	}
	return false, nil
}

func isObjectPresenceStatus(err error) bool {
	return errors.Is(err, apistatus.ErrObjectAlreadyRemoved) ||
		errors.Is(err, ierrors.ErrParentObject) ||
		errors.Is(err, apistatus.ErrObjectNotFound)
}
