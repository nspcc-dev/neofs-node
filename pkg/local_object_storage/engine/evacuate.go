package engine

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/hrw/v2"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

const defaultEvacuateBatchSize = 100

type pooledShard struct {
	shardWrapper
	pool util.WorkerPool
}

var errMustHaveTwoShards = errors.New("must have at least 1 spare shard")

// Evacuate moves data from a set of given shards to other shards available to
// this engine (so at least one shard must be available unless faultHandler is
// given). Shards being moved must be in read-only mode. Will return an error
// if unable to get an object unless ignoreErrors is set to true. If unable
// to put an object into any of the provided shards invokes faultHandler
// (if provided, fails otherwise) which can return its own error to abort
// evacuation (or nil to continue). Returns the number of evacuated objects
// (which can be non-zero even in case of error).
func (e *StorageEngine) Evacuate(shardIDs []*shard.ID, ignoreErrors bool, faultHandler func(oid.Address, *objectSDK.Object) error) (int, error) {
	sidList := make([]string, len(shardIDs))
	for i := range shardIDs {
		sidList[i] = shardIDs[i].String()
	}

	e.mtx.RLock()
	for i := range sidList {
		sh, ok := e.shards[sidList[i]]
		if !ok {
			e.mtx.RUnlock()
			return 0, errShardNotFound
		}

		if !sh.GetMode().ReadOnly() {
			e.mtx.RUnlock()
			return 0, shard.ErrMustBeReadOnly
		}
	}

	if len(e.shards)-len(sidList) < 1 && faultHandler == nil {
		e.mtx.RUnlock()
		return 0, errMustHaveTwoShards
	}

	e.log.Info("started shards evacuation", zap.Strings("shard_ids", sidList))

	// We must have all shards, to have correct information about their
	// indexes in a sorted slice and set appropriate marks in the metabase.
	// Evacuated shard is skipped during put.
	shards := make([]pooledShard, 0, len(e.shards))
	for id := range e.shards {
		shards = append(shards, pooledShard{
			shardWrapper: e.shards[id],
			pool:         e.shardPools[id],
		})
	}
	e.mtx.RUnlock()

	shardMap := make(map[string]*shard.Shard)
	for i := range sidList {
		for j := range shards {
			if shards[j].ID().String() == sidList[i] {
				shardMap[sidList[i]] = shards[j].Shard
			}
		}
	}

	var count int

mainLoop:
	for n := range sidList {
		sh := shardMap[sidList[n]]

		var c *meta.Cursor
		for {
			// TODO (@fyrchik): #1731 this approach doesn't work in degraded modes
			//  because ListWithCursor works only with the metabase.
			lst, cursor, err := sh.ListWithCursor(defaultEvacuateBatchSize, c)
			if err != nil {
				if errors.Is(err, meta.ErrEndOfListing) || errors.Is(err, shard.ErrDegradedMode) {
					continue mainLoop
				}
				return count, err
			}

			// TODO (@fyrchik): #1731 parallelize the loop
		loop:
			for i := range lst {
				addr := lst[i].Address
				addrHash := hrw.WrapBytes([]byte(addr.EncodeToString()))

				obj, err := sh.Get(addr, false)
				if err != nil {
					if ignoreErrors {
						continue
					}
					return count, err
				}

				hrw.Sort(shards, addrHash)
				for j := range shards {
					if _, ok := shardMap[shards[j].ID().String()]; ok {
						continue
					}
					putDone, exists, _ := e.putToShard(shards[j].shardWrapper, j, shards[j].pool, addr, obj, nil)
					if putDone || exists {
						if putDone {
							e.log.Debug("object is moved to another shard",
								zap.String("from", sidList[n]),
								zap.Stringer("to", shards[j].ID()),
								zap.Stringer("addr", addr))

							count++
						}
						continue loop
					}

					e.log.Debug("could not put to shard, trying another", zap.String("shard", shards[j].ID().String()))
				}

				if faultHandler == nil {
					// Do not check ignoreErrors flag here because
					// ignoring errors on put make this command kinda useless.
					return count, fmt.Errorf("%w: %s", errPutShard, lst[i])
				}

				err = faultHandler(addr, obj)
				if err != nil {
					return count, err
				}
				count++
			}

			c = cursor
		}
	}

	e.log.Info("finished shards evacuation",
		zap.Strings("shard_ids", sidList))
	return count, nil
}
