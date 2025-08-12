package engine

import (
	"errors"
	"fmt"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// TODO: docs.
// TODO: keep in sync with https://github.com/nspcc-dev/neofs-node/pull/3466.
func (e *StorageEngine) GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetDuration)() // TODO: consider other metric
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()
	if e.blockErr != nil {
		return object.Object{}, e.blockErr
	}

	// TODO: sync placement with PUT. They must sort shard equally, but now PUT sorts by part ID.
	shards := e.sortShardsFn(e, oid.NewAddress(cnr, parent))

	var partID oid.ID
loop:
	for i := range shards {
		obj, err := shards[i].GetECPart(cnr, parent, pi)
		switch {
		case err == nil:
			return obj, nil
		case errors.As(err, (*shard.ErrObjectID)(&partID)):
			shards = shards[i:]
			break loop
		case errors.Is(err, apistatus.ErrObjectAlreadyRemoved):
			return object.Object{}, err
		case errors.Is(err, meta.ErrObjectIsExpired):
			return object.Object{}, apistatus.ErrObjectNotFound // like Get
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Warn("failed to get EC part from shard, ignore",
				zap.Stringer("shardID", shards[i].ID()), zap.Error(err)) // TODO: more fields
			// TODO: need report error?
		}
	}

	for i := range shards {
		// get an object bypassing the metabase. We can miss deletion or expiration mark. Get behaves like this, so here too.
		obj, err := shards[i].Get(oid.NewAddress(cnr, partID), true)
		switch {
		case err == nil:
			return *obj, nil
		case errors.Is(err, apistatus.ErrObjectNotFound):
		default:
			e.log.Warn("failed to get EC part from shard, ignore",
				zap.Stringer("shardID", shards[i].ID()), zap.Error(err)) // TODO: more fields
			// TODO: need report error?
		}
	}

	return object.Object{}, fmt.Errorf("%w: all shards failed", apistatus.ErrObjectNotFound)
}
