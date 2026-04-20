package v2

import (
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/container/acl"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// RequestInfo groups parsed version-independent (from SDK library)
// request information and raw API request.
type RequestInfo struct {
	RequestRole acl.Role
	Operation   acl.Op // put, get, head, etc.

	Container   container.Container
	ContainerID cid.ID

	// optional for some request
	// e.g. Put, Search
	Obj *oid.ID

	SenderKey     []byte
	SenderAccount *user.ID

	Bearer *bearer.Token // bearer token of request

	SrcRequest any
}
