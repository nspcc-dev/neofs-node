package putsvc

import (
	"fmt"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/transformer"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ObjectStorage is an object storage interface.
type ObjectStorage interface {
	// Put must save passed object
	// and return any appeared error.
	Put(*object.Object) error
	// Delete must delete passed objects
	// and return any appeared error.
	Delete(tombstone oid.Address, toDelete []oid.ID) error
	// Lock must lock passed objects
	// and return any appeared error.
	Lock(locker oid.Address, toLock []oid.ID) error
	// IsLocked must clarify object's lock status.
	IsLocked(oid.Address) (bool, error)
}

type localTarget struct {
	storage ObjectStorage

	obj  *object.Object
	meta objectCore.ContentMeta
}

func (t *localTarget) WriteObject(obj *object.Object, meta objectCore.ContentMeta) error {
	t.obj = obj
	t.meta = meta

	return nil
}

func (t *localTarget) Close() (*transformer.AccessIdentifiers, error) {
	switch t.meta.Type() {
	case object.TypeTombstone:
		err := t.storage.Delete(objectCore.AddressOf(t.obj), t.meta.Objects())
		if err != nil {
			return nil, fmt.Errorf("could not delete objects from tombstone locally: %w", err)
		}
	case object.TypeLock:
		err := t.storage.Lock(objectCore.AddressOf(t.obj), t.meta.Objects())
		if err != nil {
			return nil, fmt.Errorf("could not lock object from lock objects locally: %w", err)
		}
	default:
		// objects that do not change meta storage
	}

	if err := t.storage.Put(t.obj); err != nil {
		return nil, fmt.Errorf("(%T) could not put object to local storage: %w", t, err)
	}

	id, _ := t.obj.ID()

	return new(transformer.AccessIdentifiers).
		WithSelfID(id), nil
}
