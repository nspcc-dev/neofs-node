package acl

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/acl"
	"github.com/nspcc-dev/neofs-api-go/v2/container"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/pkg/errors"
)

type (
	// ContainerGetter accesses NeoFS container storage.
	ContainerGetter interface {
		Get(*refs.ContainerID) (*container.Container, error)
	}

	Classifier interface {
		Classify(RequestV2, *refs.ContainerID) acl.Role
	}

	// BasicChecker checks basic ACL rules.
	BasicChecker struct {
		sender SenderClassifier
		next   object.Service
	}

	putStreamBasicChecker struct {
		sender SenderClassifier
		next   object.PutObjectStreamer
	}

	getStreamBasicChecker struct {
		sender SenderClassifier
		next   object.GetObjectStreamer
	}

	searchStreamBasicChecker struct {
		sender SenderClassifier
		next   object.SearchObjectStreamer
	}

	getRangeStreamBasicChecker struct {
		sender SenderClassifier
		next   object.GetRangeObjectStreamer
	}
)

var (
	ErrMalformedRequest = errors.New("malformed request")
	ErrUnknownRole      = errors.New("can't classify request sender")
)

// NewBasicChecker is a constructor for basic ACL checker of object requests.
func NewBasicChecker(c SenderClassifier, next object.Service) BasicChecker {
	return BasicChecker{
		sender: c,
		next:   next,
	}
}

func (b BasicChecker) Get(
	ctx context.Context,
	request *object.GetRequest) (object.GetObjectStreamer, error) {

	// get container address and do not panic at malformed request
	var addr *refs.Address
	if body := request.GetBody(); body == nil {
		return nil, ErrMalformedRequest
	} else {
		addr = body.GetAddress()
	}

	role := b.sender.Classify(request, addr.GetContainerID())
	if role == acl.RoleUnknown {
		return nil, ErrUnknownRole
	}

	stream, err := b.next.Get(ctx, request)
	return getStreamBasicChecker{
		sender: b.sender,
		next:   stream,
	}, err
}

func (b BasicChecker) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	streamer, err := b.next.Put(ctx)

	return putStreamBasicChecker{
		sender: b.sender,
		next:   streamer,
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
		sender: b.sender,
		next:   stream,
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
		sender: b.sender,
		next:   stream,
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
