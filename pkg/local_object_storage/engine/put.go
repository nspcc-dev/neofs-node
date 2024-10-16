package engine

import (
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	obj *objectSDK.Object

	binSet bool
	objBin []byte
	hdrLen int
}

// PutRes groups the resulting values of Put operation.
type PutRes struct{}

var errPutShard = errors.New("could not put object to any shard")

// WithObject is a Put option to set object to save.
//
// Option is required.
func (p *PutPrm) WithObject(obj *objectSDK.Object) {
	p.obj = obj
}

// SetObjectBinary allows to provide the already encoded object in
// [StorageEngine] format. Object header must be a prefix with specified length.
// If provided, the encoding step is skipped. It's the caller's responsibility
// to ensure that the data matches the object structure being processed.
func (p *PutPrm) SetObjectBinary(objBin []byte, hdrLen int) {
	p.binSet = true
	p.objBin = objBin
	p.hdrLen = hdrLen
}

// Put saves the object to local storage.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if the object has been marked as removed.
func (e *StorageEngine) Put(prm PutPrm) (res PutRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.put(prm)
		return err
	})

	return
}

func (e *StorageEngine) put(prm PutPrm) (PutRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddPutDuration)()
	}

	addr := object.AddressOf(prm.obj)

	// In #1146 this check was parallelized, however, it became
	// much slower on fast machines for 4 shards.
	_, err := e.exists(addr)
	if err != nil {
		return PutRes{}, err
	}

	finished := false
	var bestShard hashedShard
	var bestPool util.WorkerPool

	e.iterateOverSortedShards(addr, func(ind int, sh hashedShard) (stop bool) {
		e.mtx.RLock()
		pool, ok := e.shardPools[sh.ID().String()]
		if ok && bestPool == nil {
			bestShard = sh
			bestPool = pool
		}
		e.mtx.RUnlock()
		if !ok {
			// Shard was concurrently removed, skip.
			return false
		}

		exists, err := e.putToShard(sh, ind, pool, addr, prm)
		finished = err == nil || exists
		return finished
	})

	if !finished {
		err = e.putToShardWithDeadLine(bestShard, 0, bestPool, addr, prm)
		if err != nil {
			e.log.Warn("last stand to put object to the best shard",
				zap.Stringer("addr", addr),
				zap.Stringer("shard", bestShard.ID()),
				zap.Error(err))

			return PutRes{}, errPutShard
		}
	}

	return PutRes{}, nil
}

func (e *StorageEngine) putToShardWithDeadLine(sh hashedShard, ind int, pool util.WorkerPool, addr oid.Address, prm PutPrm) error {
	const deadline = 30 * time.Second
	timer := time.NewTimer(deadline)
	defer timer.Stop()

	const putCooldown = 100 * time.Millisecond
	ticker := time.NewTicker(putCooldown)
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			return fmt.Errorf("could not put object within %s", deadline)
		case <-ticker.C:
			_, err := e.putToShard(sh, ind, pool, addr, prm)
			if errors.Is(err, ants.ErrPoolOverload) {
				ticker.Reset(putCooldown)
				continue
			}

			return err
		}
	}
}

// putToShard puts object to sh.
// Return value is true iff object already exists.
func (e *StorageEngine) putToShard(sh hashedShard, ind int, pool util.WorkerPool, addr oid.Address, prm PutPrm) (bool, error) {
	var alreadyExists bool
	var errGlobal error
	id := sh.ID()

	exitCh := make(chan struct{})

	err := pool.Submit(func() {
		defer close(exitCh)

		var existPrm shard.ExistsPrm
		existPrm.SetAddress(addr)

		exists, err := sh.Exists(existPrm)
		if err != nil {
			e.log.Warn("object put: check object existence",
				zap.Stringer("addr", addr),
				zap.Stringer("shard", id),
				zap.Error(err))

			if shard.IsErrObjectExpired(err) {
				// object is already found but
				// expired => do nothing with it
				alreadyExists = true
				return
			}

			errGlobal = err

			return // this is not ErrAlreadyRemoved error so we can go to the next shard
		}

		alreadyExists = exists.Exists()
		if alreadyExists {
			if ind != 0 {
				var toMoveItPrm shard.ToMoveItPrm
				toMoveItPrm.SetAddress(addr)

				_, err = sh.ToMoveIt(toMoveItPrm)
				if err != nil {
					e.log.Warn("could not mark object for shard relocation",
						zap.Stringer("shard", id),
						zap.String("error", err.Error()),
					)
				}
			}

			e.log.Debug("object put: object already exists",
				zap.Stringer("shard", id),
				zap.Stringer("addr", addr))

			return
		}

		var putPrm shard.PutPrm
		putPrm.SetObject(prm.obj)
		if prm.binSet {
			putPrm.SetObjectBinary(prm.objBin, prm.hdrLen)
		}

		_, err = sh.Put(putPrm)
		if err != nil {
			errGlobal = err

			if errors.Is(err, shard.ErrReadOnlyMode) || errors.Is(err, blobstor.ErrNoPlaceFound) ||
				errors.Is(err, common.ErrReadOnly) || errors.Is(err, common.ErrNoSpace) {
				e.log.Warn("could not put object to shard",
					zap.Stringer("shard_id", id),
					zap.String("error", err.Error()))
				return
			}

			e.reportShardError(sh, "could not put object to shard", errGlobal)
			return
		}
	})
	if err != nil {
		e.log.Warn("object put: pool task submitting", zap.Stringer("shard", id), zap.Error(err))
		close(exitCh)

		return false, err
	}

	<-exitCh

	return alreadyExists, errGlobal
}

// Put writes provided object to local storage.
func Put(storage *StorageEngine, obj *objectSDK.Object) error {
	var putPrm PutPrm
	putPrm.WithObject(obj)

	_, err := storage.Put(putPrm)

	return err
}
