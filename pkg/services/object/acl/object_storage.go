package acl

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// sdkStorage wraps neofs-node's storage engine to
// provide SKD-only API for Head operation.
type sdkStorage struct {
	s *engine.StorageEngine
}

// Head reads object header from local storage by provided address
// and returns SDK object structure.
func (sw sdkStorage) Head(a *objectSDK.Address) (*objectSDK.Object, error) {
	obj, err := engine.Head(sw.s, a)
	if err != nil {
		return nil, err
	}

	return obj.SDK(), nil
}
