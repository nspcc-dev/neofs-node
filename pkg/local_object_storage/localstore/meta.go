package localstore

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/pkg/errors"
)

// StoreEpochValue is a context key of object storing epoch number.
const StoreEpochValue = "store epoch"

func (l *localstore) Meta(key refs.Address) (*ObjectMeta, error) {
	var (
		err  error
		meta ObjectMeta
		k, v []byte
	)

	k, err = key.Hash()
	if err != nil {
		return nil, errors.Wrap(err, "Localstore Meta failed on key.Marshal")
	}

	v, err = l.metaBucket.Get(k)
	if err != nil {
		return nil, errors.Wrap(err, "Localstore Meta failed on metaBucket.Get")
	}

	if err := meta.Unmarshal(v); err != nil {
		return nil, errors.Wrap(err, "Localstore Metafailed on ObjectMeta.Unmarshal")
	}

	return &meta, nil
}

func metaFromObject(ctx context.Context, obj *Object) *ObjectMeta {
	meta := new(ObjectMeta)
	o := *obj
	meta.Object = &o
	meta.Object.Payload = nil
	meta.PayloadSize = uint64(len(obj.Payload))
	meta.PayloadHash = hash.Sum(obj.Payload)

	storeEpoch, ok := ctx.Value(StoreEpochValue).(uint64)
	if ok {
		meta.StoreEpoch = storeEpoch
	}

	return meta
}
