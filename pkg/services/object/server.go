package object

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/util"
)

// GetObjectStream is an interface of NeoFS API v2 compatible object streamer.
type GetObjectStream interface {
	util.ServerStream
	Send(*object.GetResponse) error
}

// ServiceServer is an interface of utility
// serving v2 Object service.
type ServiceServer interface {
	Get(*object.GetRequest, GetObjectStream) error
	Put(context.Context) (object.PutObjectStreamer, error)
	Head(context.Context, *object.HeadRequest) (*object.HeadResponse, error)
	Search(context.Context, *object.SearchRequest) (object.SearchObjectStreamer, error)
	Delete(context.Context, *object.DeleteRequest) (*object.DeleteResponse, error)
	GetRange(context.Context, *object.GetRangeRequest) (object.GetRangeObjectStreamer, error)
	GetRangeHash(context.Context, *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error)
}
