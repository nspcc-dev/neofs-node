package localstore

import (
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/pkg/errors"
)

func (l *localstore) Has(key refs.Address) (bool, error) {
	var (
		err error
		k   []byte
	)

	k, err = key.Hash()
	if err != nil {
		return false, errors.Wrap(err, "localstore.Has failed on key.Marshal")
	}

	return l.metaBucket.Has(k) && l.blobBucket.Has(k), nil
}
