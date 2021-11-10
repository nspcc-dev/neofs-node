package engine

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr []*objectSDK.Address
}

// DeleteRes groups resulting values of Delete operation.
type DeleteRes struct{}

// WithAddresses is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) WithAddresses(addr ...*objectSDK.Address) *DeletePrm {
	if p != nil {
		p.addr = append(p.addr, addr...)
	}

	return p
}

// Delete marks the objects to be removed.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Delete(prm *DeletePrm) (res *DeleteRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.delete(prm)
		return err
	})

	return
}

func (e *StorageEngine) delete(prm *DeletePrm) (*DeleteRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddDeleteDuration)()
	}

	shPrm := new(shard.InhumePrm)
	existsPrm := new(shard.ExistsPrm)

	for i := range prm.addr {
		e.iterateOverSortedShards(prm.addr[i], func(_ int, sh *shard.Shard) (stop bool) {
			resExists, err := sh.Exists(existsPrm.WithAddress(prm.addr[i]))
			if err != nil {
				// TODO: smth wrong with shard, need to be processed
				e.log.Warn("could not check object existence",
					zap.Stringer("shard", sh.ID()),
					zap.String("error", err.Error()),
				)

				return false
			} else if !resExists.Exists() {
				return false
			}

			_, err = sh.Inhume(shPrm.MarkAsGarbage(prm.addr[i]))
			if err != nil {
				// TODO: smth wrong with shard, need to be processed
				e.log.Warn("could not inhume object in shard",
					zap.Stringer("shard", sh.ID()),
					zap.String("error", err.Error()),
				)
			}

			return err == nil
		})
	}

	return nil, nil
}
