package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addr []oid.Address
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct{}

// WithAddresses is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) WithAddresses(addr ...oid.Address) *DeletePrm {
	if p != nil {
		p.addr = append(p.addr, addr...)
	}

	return p
}

// Delete marks the objects to be removed.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// Returns apistatus.ObjectLocked if at least one object is locked.
// In this case no object from the list is marked to be deleted.
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
	var locked struct {
		is  bool
		err apistatus.ObjectLocked
	}

	for i := range prm.addr {
		e.iterateOverSortedShards(prm.addr[i], func(_ int, sh hashedShard) (stop bool) {
			resExists, err := sh.Exists(existsPrm.WithAddress(prm.addr[i]))
			if err != nil {
				e.reportShardError(sh, "could not check object existence", err)
				return false
			} else if !resExists.Exists() {
				return false
			}

			_, err = sh.Inhume(shPrm.MarkAsGarbage(prm.addr[i]))
			if err != nil {
				e.reportShardError(sh, "could not inhume object in shard", err)

				locked.is = errors.As(err, &locked.err)

				return locked.is
			}

			return true
		})
	}

	if locked.is {
		return nil, locked.err
	}

	return nil, nil
}
