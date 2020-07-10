package localstore

import (
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/pkg/errors"
)

func (l *localstore) Get(key refs.Address) (*Object, error) {
	var (
		err  error
		k, v []byte
		o    = new(Object)
	)

	k, err = key.Hash()
	if err != nil {
		return nil, errors.Wrap(err, "Localstore Get failed on key.Marshal")
	}

	v, err = l.blobBucket.Get(k)
	if err != nil {
		return nil, errors.Wrap(err, "Localstore Get failed on blobBucket.Get")
	}

	if err = o.Unmarshal(v); err != nil {
		return nil, errors.Wrap(err, "Localstore Get failed on Object.Unmarshal")
	}

	return o, nil
}
