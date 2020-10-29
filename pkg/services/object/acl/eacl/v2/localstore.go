package v2

import (
	"io"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
)

type localStorage struct {
	ls *localstore.Storage
}

func (s *localStorage) Head(addr *objectSDK.Address) (*object.Object, error) {
	if s.ls == nil {
		return nil, io.ErrUnexpectedEOF
	}

	return s.ls.Head(addr)
}
