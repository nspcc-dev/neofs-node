package main

import (
	"context"
	"errors"
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-api-go/v2/session"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
)

type objectExecutor struct{}

type inMemBucket struct {
	bucket.Bucket
	*sync.RWMutex
	items map[string][]byte
}

func newBucket() bucket.Bucket {
	return &inMemBucket{
		RWMutex: new(sync.RWMutex),
		items:   map[string][]byte{},
	}
}

func (b *inMemBucket) Get(key []byte) ([]byte, error) {
	b.RLock()
	v, ok := b.items[base58.Encode(key)]
	b.RUnlock()

	if !ok {
		return nil, errors.New("not found")
	}

	return v, nil
}

func (b *inMemBucket) Set(key, value []byte) error {
	k := base58.Encode(key)

	b.Lock()
	b.items[k] = makeCopy(value)
	b.Unlock()

	return nil
}

func (b *inMemBucket) Iterate(handler bucket.FilterHandler) error {
	if handler == nil {
		return bucket.ErrNilFilterHandler
	}

	b.RLock()
	for key, val := range b.items {
		k, err := base58.Decode(key)
		if err != nil {
			panic(err)
		}

		v := makeCopy(val)

		if !handler(k, v) {
			return bucket.ErrIteratingAborted
		}
	}
	b.RUnlock()

	return nil
}

func makeCopy(val []byte) []byte {
	tmp := make([]byte, len(val))
	copy(tmp, val)

	return tmp
}

func (*objectExecutor) Get(context.Context, *object.GetRequestBody) (objectService.GetObjectBodyStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) Put(context.Context) (objectService.PutObjectBodyStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) Head(context.Context, *object.HeadRequestBody) (*object.HeadResponseBody, error) {
	return nil, errors.New("unimplemented service call")
}

func (s *objectExecutor) Search(context.Context, *object.SearchRequestBody) (objectService.SearchObjectBodyStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) Delete(_ context.Context, body *object.DeleteRequestBody) (*object.DeleteResponseBody, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) GetRange(_ context.Context, body *object.GetRangeRequestBody) (objectService.GetRangeObjectBodyStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectExecutor) GetRangeHash(context.Context, *object.GetRangeHashRequestBody) (*object.GetRangeHashResponseBody, error) {
	return nil, errors.New("unimplemented service call")
}

func initObjectService(c *cfg) {
	metaHdr := new(session.ResponseMetaHeader)
	xHdr := new(session.XHeader)
	xHdr.SetKey("test X-Header key for Object service")
	xHdr.SetValue("test X-Header value for Object service")
	metaHdr.SetXHeaders([]*session.XHeader{xHdr})

	objectGRPC.RegisterObjectServiceServer(c.cfgGRPC.server,
		objectTransportGRPC.New(
			objectService.NewSignService(
				c.key,
				objectService.NewExecutionService(
					new(objectExecutor),
					metaHdr,
				),
			),
		),
	)
}
