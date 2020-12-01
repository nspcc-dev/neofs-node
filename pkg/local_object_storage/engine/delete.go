package engine

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr *objectSDK.Address
}

// DeleteRes groups resulting values of Delete operation.
type DeleteRes struct{}

// WithAddress is a Delete option to set the address of the object to delete.
//
// Option is required.
func (p *DeletePrm) WithAddress(addr *objectSDK.Address) *DeletePrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Delete marks object to delete from local storage.
//
// Returns any error encountered that did not allow to completely
// mark the object to delete.
func (e *StorageEngine) Delete(prm *DeletePrm) (*DeleteRes, error) {
	shPrm := new(shard.DeletePrm).
		WithAddress(prm.addr)

	e.iterateOverSortedShards(prm.addr, func(_ int, sh *shard.Shard) (stop bool) {
		_, err := sh.Delete(shPrm)
		if err != nil {
			// TODO: smth wrong with shard, need to be processed
			e.log.Warn("could not get object from shard",
				zap.Stringer("shard", sh.ID()),
				zap.String("error", err.Error()),
			)
		}

		return false
	})

	return nil, nil
}
