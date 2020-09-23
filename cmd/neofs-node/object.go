package main

import (
	"context"
	"errors"
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/v2/object"
	objectGRPC "github.com/nspcc-dev/neofs-api-go/v2/object/grpc"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket"
	objectTransportGRPC "github.com/nspcc-dev/neofs-node/pkg/network/transport/object/grpc"
	objectService "github.com/nspcc-dev/neofs-node/pkg/services/object"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head/v2"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put/v2"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search/v2"
)

type objectSvc struct {
	put *putsvc.Service

	search *searchsvc.Service

	head *headsvc.Service
}

type inMemBucket struct {
	bucket.Bucket
	*sync.RWMutex
	items map[string][]byte
}

type maxSzSrc struct {
	v uint64
}

func (s *maxSzSrc) MaxObjectSize() uint64 {
	return s.v
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

func (s *objectSvc) Put(ctx context.Context) (object.PutObjectStreamer, error) {
	return s.put.Put(ctx)
}

func (s *objectSvc) Head(ctx context.Context, req *object.HeadRequest) (*object.HeadResponse, error) {
	return s.head.Head(ctx, req)
}

func (s *objectSvc) Search(ctx context.Context, req *object.SearchRequest) (object.SearchObjectStreamer, error) {
	return s.search.Search(ctx, req)
}

func (*objectSvc) Get(context.Context, *object.GetRequest) (object.GetObjectStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectSvc) Delete(context.Context, *object.DeleteRequest) (*object.DeleteResponse, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectSvc) GetRange(context.Context, *object.GetRangeRequest) (object.GetRangeObjectStreamer, error) {
	return nil, errors.New("unimplemented service call")
}

func (*objectSvc) GetRangeHash(context.Context, *object.GetRangeHashRequest) (*object.GetRangeHashResponse, error) {
	return nil, errors.New("unimplemented service call")
}

func initObjectService(c *cfg) {
	svc := new(objectSvc)

	objectGRPC.RegisterObjectServiceServer(c.cfgGRPC.server,
		objectTransportGRPC.New(
			objectService.NewSignService(
				c.key,
				svc,
			),
		),
	)
}
