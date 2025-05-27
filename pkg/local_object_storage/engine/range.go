package engine

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-sdk-go/debugprint"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// GetRange reads a part of an object from local storage. Zero length is
// interpreted as requiring full object length independent of the offset.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object is inhumed.
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) GetRange(ctx context.Context, addr oid.Address, offset uint64, length uint64) ([]byte, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddRangeDuration)()
	}
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var (
		err  error
		data []byte
	)

	err = e.get(addr, func(sh *shard.Shard, ignoreMetadata bool) error {
		st := debugprint.LogRequestStageStart(ctx, "shard RANGE")
		res, err := sh.GetRange(ctx, addr, offset, length, ignoreMetadata)
		debugprint.LogRequestStageFinish(st)
		if err == nil {
			data = res.Payload()
		}
		return err
	})
	return data, err
}
