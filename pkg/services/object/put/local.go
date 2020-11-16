package putsvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/pkg/errors"
)

type localTarget struct {
	storage *localstore.Storage

	obj *object.RawObject

	payload []byte
}

func (t *localTarget) WriteHeader(obj *object.RawObject) error {
	t.obj = obj

	t.payload = make([]byte, 0, obj.PayloadSize())

	return nil
}

func (t *localTarget) Write(p []byte) (n int, err error) {
	t.payload = append(t.payload, p...)

	return len(p), nil
}

func (t *localTarget) Close() (*transformer.AccessIdentifiers, error) {
	if err := t.storage.Put(t.obj.Object()); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not put object to local storage", t)
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(t.obj.ID()), nil
}
