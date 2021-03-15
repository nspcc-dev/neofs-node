package engine

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	addr *objectSDK.Address
}

// ExistsRes groups resulting values of Exists operation.
type ExistsRes struct {
	ex bool
}

// WithAddress is an Exists option to set object checked for existence.
func (p *ExistsPrm) WithAddress(addr *objectSDK.Address) *ExistsPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Exists returns the fact that the object is in the storage engine.
func (p *ExistsRes) Exists() bool {
	return p.ex
}

// Exists checks if object is presented in storage engine
//
// Returns any error encountered that does not allow to
// unambiguously determine the presence of an object.
func (e *StorageEngine) Exists(prm *ExistsPrm) (*ExistsRes, error) {
	if e.enableMetrics {
		defer elapsed(existsDuration)()
	}

	exists, err := e.exists(prm.addr)

	return &ExistsRes{
		ex: exists,
	}, err
}

func (e *StorageEngine) exists(addr *objectSDK.Address) (bool, error) {
	shPrm := new(shard.ExistsPrm).WithAddress(addr)
	alreadyRemoved := false
	exists := false

	e.iterateOverSortedShards(addr, func(_ int, sh *shard.Shard) (stop bool) {
		res, err := sh.Exists(shPrm)
		if err != nil {
			if errors.Is(err, object.ErrAlreadyRemoved) {
				alreadyRemoved = true

				return true
			}

			// TODO: smth wrong with shard, need to be processed
			e.log.Warn("could not check existence of object in shard",
				zap.Stringer("shard", sh.ID()),
				zap.String("error", err.Error()),
			)
		}

		if res != nil && !exists {
			exists = res.Exists()
		}

		return false
	})

	if alreadyRemoved {
		return false, object.ErrAlreadyRemoved
	}

	return exists, nil
}
