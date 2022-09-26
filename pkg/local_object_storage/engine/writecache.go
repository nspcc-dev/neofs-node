package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
)

// FlushWriteCachePrm groups the parameters of FlushWriteCache operation.
type FlushWriteCachePrm struct {
	shardID      *shard.ID
	ignoreErrors bool
}

// SetShardID is an option to set shard ID.
//
// Option is required.
func (p *FlushWriteCachePrm) SetShardID(id *shard.ID) {
	p.shardID = id
}

// SetIgnoreErrors sets errors ignore flag..
func (p *FlushWriteCachePrm) SetIgnoreErrors(ignore bool) {
	p.ignoreErrors = ignore
}

// FlushWriteCacheRes groups the resulting values of FlushWriteCache operation.
type FlushWriteCacheRes struct{}

// FlushWriteCache flushes write-cache on a single shard.
func (e *StorageEngine) FlushWriteCache(p FlushWriteCachePrm) (FlushWriteCacheRes, error) {
	e.mtx.RLock()
	sh, ok := e.shards[p.shardID.String()]
	e.mtx.RUnlock()

	if !ok {
		return FlushWriteCacheRes{}, errShardNotFound
	}

	var prm shard.FlushWriteCachePrm
	prm.SetIgnoreErrors(p.ignoreErrors)

	return FlushWriteCacheRes{}, sh.FlushWriteCache(prm)
}
