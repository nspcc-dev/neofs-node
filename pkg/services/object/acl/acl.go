package acl

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
)

type (
	// ContainerGetter accesses NeoFS container storage.
	ContainerGetter interface{}

	// BasicChecker checks basic ACL rules.
	BasicChecker struct {
		containers ContainerGetter
		next       object.Service
	}

	putStreamBasicChecker struct {
		containers ContainerGetter
		next       object.PutObjectStreamer
	}

	getStreamBasicChecker struct {
		containers ContainerGetter
		next       object.GetObjectStreamer
	}

	searchStreamBasicChecker struct {
		containers ContainerGetter
		next       object.SearchObjectStreamer
	}

	getRangeStreamBasicChecker struct {
		containers ContainerGetter
		next       object.GetRangeObjectStreamer
	}
)

// NewBasicChecker is a constructor for basic ACL checker of object requests.
func NewBasicChecker(cnr ContainerGetter, next object.Service) BasicChecker {
	return BasicChecker{
		containers: cnr,
		next:       next,
	}
}

func (b BasicChecker) Get(
	ctx context.Context,
	request *object.GetRequest) (object.GetObjectStreamer, error) {

	stream, err := b.next.Get(ctx, request)
	return getStreamBasicChecker{
		containers: b.containers,
		next:       stream,
	}, err
}

func (b BasicChecker) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	streamer, err := b.next.Put(ctx)

	return putStreamBasicChecker{
		containers: b.containers,
		next:       streamer,
	}, err
}

func (b BasicChecker) Head(
	ctx context.Context,
	request *object.HeadRequest) (*object.HeadResponse, error) {

	return b.next.Head(ctx, request)
}

func (b BasicChecker) Search(
	ctx context.Context,
	request *object.SearchRequest) (object.SearchObjectStreamer, error) {

	stream, err := b.next.Search(ctx, request)
	return searchStreamBasicChecker{
		containers: b.containers,
		next:       stream,
	}, err
}

func (b BasicChecker) Delete(
	ctx context.Context,
	request *object.DeleteRequest) (*object.DeleteResponse, error) {

	return b.next.Delete(ctx, request)
}

func (b BasicChecker) GetRange(
	ctx context.Context,
	request *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {

	stream, err := b.next.GetRange(ctx, request)
	return getRangeStreamBasicChecker{
		containers: b.containers,
		next:       stream,
	}, err
}

func (b BasicChecker) GetRangeHash(
	ctx context.Context,
	request *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {

	return b.next.GetRangeHash(ctx, request)
}

func (p putStreamBasicChecker) Send(request *object.PutRequest) error {
	return p.next.Send(request)
}

func (p putStreamBasicChecker) CloseAndRecv() (*object.PutResponse, error) {
	return p.next.CloseAndRecv()
}

func (g getStreamBasicChecker) Recv() (*object.GetResponse, error) {
	return g.next.Recv()
}

func (s searchStreamBasicChecker) Recv() (*object.SearchResponse, error) {
	return s.next.Recv()
}

func (g getRangeStreamBasicChecker) Recv() (*object.GetRangeResponse, error) {
	return g.next.Recv()
}
