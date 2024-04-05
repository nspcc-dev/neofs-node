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

	e.iterateOverSortedShards(addr, func(ind int, sh hashedShard) (stop bool) {
		e.mtx.RLock()
		pool, ok := e.shardPools[sh.ID().String()]
		e.mtx.RUnlock()
		if !ok {
			// Shard was concurrently removed, skip.
			return false
		}

		putDone, exists := e.putToShard(sh, ind, pool, addr, prm)
		finished = putDone || exists
		return finished
	})

	if !finished {
		err = errPutShard
	}

	return PutRes{}, err
}

// putToShard puts object to sh.
// First return value is true iff put has been successfully done.
// Second return value is true iff object already exists.
func (e *StorageEngine) putToShard(sh hashedShard, ind int, pool util.WorkerPool, addr oid.Address, prm PutPrm) (bool, bool) {
	var putSuccess, alreadyExists bool

	exitCh := make(chan struct{})

	if err := pool.Submit(func() {
		defer close(exitCh)

		var existPrm shard.ExistsPrm
		existPrm.SetAddress(addr)

		exists, err := sh.Exists(existPrm)
		if err != nil {
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

			return
		}

		var putPrm shard.PutPrm
		putPrm.SetObject(prm.obj)
		if prm.binSet {
			putPrm.SetObjectBinary(prm.objBin, prm.hdrLen)
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
		close(exitCh)
	}

	<-exitCh

	return putSuccess, alreadyExists
}

// Put writes provided object to local storage.
func Put(storage *StorageEngine, obj *objectSDK.Object) error {
	var putPrm PutPrm
	putPrm.WithObject(obj)

	_, err := storage.Put(putPrm)

	return err
}
