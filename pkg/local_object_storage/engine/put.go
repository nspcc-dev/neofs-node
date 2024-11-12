package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

var errPutShard = errors.New("could not put object to any shard")

// Put saves an object to local storage. objBin and hdrLen parameters are
// optional and used to optimize out object marshaling, when used both must
// be valid.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if the object has been marked as removed.
func (e *StorageEngine) Put(obj *objectSDK.Object, objBin []byte, hdrLen int) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddPutDuration)()
	}

	return e.execIfNotBlocked(func() error {
		return e.put(obj, objBin, hdrLen)
	})
}

func (e *StorageEngine) put(obj *objectSDK.Object, objBin []byte, hdrLen int) error {
	addr := object.AddressOf(obj)

	// In #1146 this check was parallelized, however, it became
	// much slower on fast machines for 4 shards.
	_, err := e.exists(addr)
	if err != nil {
		return err
	}

	finished := false

	e.iterateOverSortedShards(addr, func(ind int, sh hashedShard) (stop bool) {
		e.mtx.RLock()
		pool, ok := e.shardPools[sh.ID().String()]
		e.mtx.RUnlock()
		if !ok {
			// Shard was concurrently removed, skip.
			return false
		}

		putDone, exists := e.putToShard(sh, ind, pool, addr, obj, objBin, hdrLen)
		finished = putDone || exists
		return finished
	})

	if !finished {
		err = errPutShard
	}

	return err
}

// putToShard puts object to sh.
// First return value is true iff put has been successfully done.
// Second return value is true iff object already exists.
func (e *StorageEngine) putToShard(sh hashedShard, ind int, pool util.WorkerPool, addr oid.Address, obj *objectSDK.Object, objBin []byte, hdrLen int) (bool, bool) {
	var putSuccess, alreadyExists bool

	exitCh := make(chan struct{})

	if err := pool.Submit(func() {
		defer close(exitCh)

		var existPrm shard.ExistsPrm
		existPrm.SetAddress(addr)

		exists, err := sh.Exists(existPrm)
		if err != nil {
			e.log.Warn("object put: check object existence",
				zap.Stringer("addr", addr),
				zap.Stringer("shard", sh.ID()),
				zap.Error(err))

			if shard.IsErrObjectExpired(err) {
				// object is already found but
				// expired => do nothing with it
				alreadyExists = true
			}

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
						zap.Stringer("shard", sh.ID()),
						zap.String("error", err.Error()),
					)
				}
			}

			e.log.Debug("object put: object already exists",
				zap.Stringer("shard", sh.ID()),
				zap.Stringer("addr", addr))

			return
		}

		var putPrm shard.PutPrm
		putPrm.SetObject(obj)
		if objBin != nil {
			putPrm.SetObjectBinary(objBin, hdrLen)
		}

		_, err = sh.Put(putPrm)
		if err != nil {
			if errors.Is(err, shard.ErrReadOnlyMode) || errors.Is(err, blobstor.ErrNoPlaceFound) ||
				errors.Is(err, common.ErrReadOnly) || errors.Is(err, common.ErrNoSpace) {
				e.log.Warn("could not put object to shard",
					zap.Stringer("shard_id", sh.ID()),
					zap.String("error", err.Error()))
				return
			}

			e.reportShardError(sh, "could not put object to shard", err)
			return
		}

		putSuccess = true
	}); err != nil {
		e.log.Warn("object put: pool task submitting", zap.Error(err))
		close(exitCh)
	}

	<-exitCh

	return putSuccess, alreadyExists
}
