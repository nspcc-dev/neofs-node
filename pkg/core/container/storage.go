package containercore

import (
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
)

// Source is an interface that wraps
// basic container receiving method.
type Source interface {
	// Get reads the container from the storage by its identifier. Get returns
	// [apistatus.ErrContainerNotFound] if the container is not in the storage.
	Get(cid.ID) (container.Container, error)
}

// EACLSource is the interface that wraps
// basic methods of extended ACL table source.
type EACLSource interface {
	// GetEACL reads the table from the source by identifier. GetEACL returns
	// [apistatus.ErrEACLNotFound] if requested eACL table is not in source.
	GetEACL(cid.ID) (eacl.Table, error)
}
