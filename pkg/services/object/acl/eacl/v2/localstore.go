package v2

import (
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type localStorage struct {
	ls *engine.StorageEngine
}

func (s *localStorage) Head(addr oid.Address) (*objectSDK.Object, error) {
	if s.ls == nil {
		return nil, io.ErrUnexpectedEOF
	}

	return engine.Head(s.ls, addr)
}
