package putsvc

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// ObjectStorage is an object storage interface.
type ObjectStorage interface {
	// Put must save passed object
	// and return any appeared error.
	Put(o *objectSDK.Object) error
}

type localTarget struct {
	storage ObjectStorage

	obj *object.Object
}

func (t *localTarget) WriteHeader(obj *object.Object) error {
	t.obj = obj

	return nil
}

func (t *localTarget) Close() (*transformer.AccessIdentifiers, error) {
	if err := t.storage.Put(t.obj); err != nil {
		return nil, fmt.Errorf("(%T) could not put object to local storage: %w", t, err)
	}

	return new(transformer.AccessIdentifiers).
		WithSelfID(t.obj.ID()), nil
}
