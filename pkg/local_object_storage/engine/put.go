package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	obj *objectSDK.Object
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
		pool := e.shardPools[sh.ID().String()]
		e.mtx.RUnlock()

		finished = e.putToShard(sh.Shard, ind, pool, addr, prm.obj)
		return finished
	})

	if !finished {
		err = errPutShard
	}

	return PutRes{}, err
}

func (e *StorageEngine) putToShard(sh *shard.Shard, ind int, pool util.WorkerPool, addr oid.Address, obj *objectSDK.Object) bool {
	var finished bool

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
				finished = true
			}

			return // this is not ErrAlreadyRemoved error so we can go to the next shard
		}

		if exists.Exists() {
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

			finished = true

			return
		}

		var putPrm shard.PutPrm
		putPrm.SetObject(obj)

		_, err = sh.Put(putPrm)
		if err != nil {
			e.log.Warn("could not put object in shard",
				zap.Stringer("shard", sh.ID()),
				zap.String("error", err.Error()))

			return
		}

		finished = true
	}); err != nil {
		close(exitCh)
	}

	<-exitCh

	return finished
}

// Put writes provided object to local storage.
func Put(storage *StorageEngine, obj *objectSDK.Object) error {
	var putPrm PutPrm
	putPrm.WithObject(obj)

	_, err := storage.Put(putPrm)

	return err
}
