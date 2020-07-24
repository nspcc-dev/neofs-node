package localstore

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/pkg/errors"
)

func (l *localstore) PRead(ctx context.Context, key Address, rng object.Range) ([]byte, error) {
	var (
		err  error
		k, v []byte
		obj  Object
	)

	k, err = key.Hash()
	if err != nil {
		return nil, errors.Wrap(err, "Localstore Get failed on key.Marshal")
	}

	v, err = l.blobBucket.Get(k)
	if err != nil {
		return nil, errors.Wrap(err, "Localstore Get failed on blobBucket.Get")
	}

	if err := obj.Unmarshal(v); err != nil {
		return nil, errors.Wrap(err, "Localstore Get failed on object.Unmarshal")
	}

	if rng.Offset+rng.Length > uint64(len(obj.Payload)) {
		return nil, ErrOutOfRange
	}

	return obj.Payload[rng.Offset : rng.Offset+rng.Length], nil
}
