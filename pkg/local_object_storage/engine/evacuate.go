package engine

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/hrw"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"go.uber.org/zap"
)

// EvacuateShardPrm represents parameters for the EvacuateShard operation.
type EvacuateShardPrm struct {
	shardID      *shard.ID
	ignoreErrors bool
}

// EvacuateShardRes represents result of the EvacuateShard operation.
type EvacuateShardRes struct {
	count int
}

// WithShardID sets shard ID.
func (p *EvacuateShardPrm) WithShardID(id *shard.ID) {
	p.shardID = id
}

// WithIgnoreErrors sets flag to ignore errors.
func (p *EvacuateShardPrm) WithIgnoreErrors(ignore bool) {
	p.ignoreErrors = ignore
}

// Count returns amount of evacuated objects.
func (p EvacuateShardRes) Count() int {
	return p.count
}

const defaultEvacuateBatchSize = 100

type pooledShard struct {
	hashedShard
	pool util.WorkerPool
}

var errMustHaveTwoShards = errors.New("amount of shards must be > 2")

// Evacuate moves data from one shard to the others.
// The shard being moved must be in read-only mode.
func (e *StorageEngine) Evacuate(prm EvacuateShardPrm) (EvacuateShardRes, error) {
	sid := prm.shardID.String()

	e.mtx.RLock()
	sh, ok := e.shards[sid]
	if !ok {
		e.mtx.RUnlock()
		return EvacuateShardRes{}, errShardNotFound
	}

	if len(e.shards) < 2 {
		e.mtx.RUnlock()
		return EvacuateShardRes{}, errMustHaveTwoShards
	}

	if !sh.GetMode().ReadOnly() {
		e.mtx.RUnlock()
		return EvacuateShardRes{}, shard.ErrMustBeReadOnly
	}

	// We must have all shards, to have correct information about their
	// indexes in a sorted slice and set appropriate marks in the metabase.
	// Evacuated shard is skipped during put.
	shards := make([]pooledShard, 0, len(e.shards))
	for id := range e.shards {
		shards = append(shards, pooledShard{
			hashedShard: hashedShard(e.shards[id]),
			pool:        e.shardPools[id],
		})
	}
	e.mtx.RUnlock()

	weights := make([]float64, 0, len(shards))
	for i := range shards {
		weights = append(weights, e.shardWeight(shards[i].Shard))
	}

	var listPrm shard.ListWithCursorPrm
	listPrm.WithCount(defaultEvacuateBatchSize)

	var c *meta.Cursor
	var res EvacuateShardRes
	for {
		listPrm.WithCursor(c)

		// TODO (@fyrchik): #1731 this approach doesn't work in degraded modes
		//  because ListWithCursor works only with the metabase.
		listRes, err := sh.Shard.ListWithCursor(listPrm)
		if err != nil {
			if errors.Is(err, meta.ErrEndOfListing) {
				return res, nil
			}
			return res, err
		}

		// TODO (@fyrchik): #1731 parallelize the loop
		lst := listRes.AddressList()

	loop:
		for i := range lst {
			var getPrm shard.GetPrm
			getPrm.SetAddress(lst[i])

			getRes, err := sh.Get(getPrm)
			if err != nil {
				if prm.ignoreErrors {
					continue
				}
				return res, err
			}

			hrw.SortSliceByWeightValue(shards, weights, hrw.Hash([]byte(lst[i].EncodeToString())))
			for j := range shards {
				if shards[j].ID().String() == sid {
					continue
				}
				ok := e.putToShard(shards[j].Shard, j, shards[j].pool, lst[i], getRes.Object())
				if ok {
					e.log.Debug("object is moved to another shard",
						zap.String("from", sid),
						zap.Stringer("to", shards[j].ID()),
						zap.Stringer("addr", lst[i]))

					res.count++
					continue loop
				}
			}

			// TODO (@fyrchik): #1731 try replicating to another node.
			//  The suggestion is to have prm.handler which is called
			//  if a Put has failed.

			// Do not check ignoreErrors flag here because
			// ignoring errors on put make this command kinda useless.
			return res, fmt.Errorf("%w: %s", errPutShard, lst[i])
		}

		c = listRes.Cursor()
	}
}
