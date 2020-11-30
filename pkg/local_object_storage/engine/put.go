package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	obj *object.Object
}

// PutRes groups resulting values of Put operation.
type PutRes struct{}

var errPutShard = errors.New("could not put object to any shard")

// WithObject is a Put option to set object to save.
//
// Option is required.
func (p *PutPrm) WithObject(obj *object.Object) *PutPrm {
	if p != nil {
		p.obj = obj
	}

	return p
}

// Put saves the object to local storage.
//
// Returns any error encountered that
// did not allow to completely save the object.
func (e *StorageEngine) Put(prm *PutPrm) (*PutRes, error) {
	// choose shards through sorting by weight
	sortedShards := e.sortShardsByWeight(prm.obj.Address())

	// check object existence
	if e.objectExists(prm.obj, sortedShards) {
		return nil, nil
	}

	shPrm := new(shard.PutPrm)

	// save the object into the "largest" possible shard
	for _, sh := range sortedShards {
		_, err := sh.sh.Put(
			shPrm.WithObject(prm.obj),
		)

		if err != nil {
			// TODO: smth wrong with shard, need to be processed
			e.log.Warn("could not save object in shard",
				zap.Stringer("shard", sh.sh.ID()),
				zap.String("error", err.Error()),
			)
		} else {
			return nil, nil
		}
	}

	return nil, errPutShard
}

func (e *StorageEngine) objectExists(obj *object.Object, shards []hashedShard) bool {
	exists := false

	for _, sh := range shards {
		res, err := sh.sh.Exists(
			new(shard.ExistsPrm).
				WithAddress(obj.Address()),
		)

		if err != nil {
			// TODO: smth wrong with shard, need to be processed
			e.log.Warn("could not check object existence",
				zap.String("error", err.Error()),
			)

			continue
		}

		if exists = res.Exists(); exists {
			break
		}
	}

	return exists
}

// Put writes provided object to local storage.
func Put(storage *StorageEngine, obj *object.Object) error {
	_, err := storage.Put(new(PutPrm).
		WithObject(obj),
	)

	return err
}
