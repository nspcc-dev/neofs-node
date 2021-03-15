package container

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/container"
)

// Server is an interface of the NeoFS API Container service server
type Server interface {
	Put(context.Context, *container.PutRequest) (*container.PutResponse, error)
	Get(context.Context, *container.GetRequest) (*container.GetResponse, error)
	Delete(context.Context, *container.DeleteRequest) (*container.DeleteResponse, error)
	List(context.Context, *container.ListRequest) (*container.ListResponse, error)
	SetExtendedACL(context.Context, *container.SetExtendedACLRequest) (*container.SetExtendedACLResponse, error)
	GetExtendedACL(context.Context, *container.GetExtendedACLRequest) (*container.GetExtendedACLResponse, error)
	AnnounceUsedSpace(context.Context, *container.AnnounceUsedSpaceRequest) (*container.AnnounceUsedSpaceResponse, error)
}
