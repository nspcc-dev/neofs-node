package v2

import (
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

type localStorage struct {
	ls *engine.StorageEngine
}

func (s *localStorage) Head(addr *objectSDK.Address) (*object.Object, error) {
	if s.ls == nil {
		return nil, io.ErrUnexpectedEOF
	}

	return engine.Head(s.ls, addr)
}
