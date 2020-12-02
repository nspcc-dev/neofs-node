package getsvc

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
)

type simpleObjectWriter struct {
	obj *object.RawObject

	payload []byte
}

type clientCacheWrapper struct {
	cache *cache.ClientCache

	opts []client.Option
}

type clientWrapper struct {
	client *client.Client
}

type storageEngineWrapper struct {
	engine *engine.StorageEngine
}

type headSvcWrapper struct {
	svc *headsvc.Service
}

func newSimpleObjectWriter() *simpleObjectWriter {
	return new(simpleObjectWriter)
}

func (s *simpleObjectWriter) WriteHeader(obj *object.Object) error {
	s.obj = object.NewRawFromObject(obj)

	s.payload = make([]byte, 0, obj.PayloadSize())

	return nil
}

func (s *simpleObjectWriter) WriteChunk(p []byte) error {
	s.payload = append(s.payload, p...)
	return nil
}

func (s *simpleObjectWriter) Close() error {
	return nil
}

func (s *simpleObjectWriter) object() *object.Object {
	if len(s.payload) > 0 {
		s.obj.SetPayload(s.payload)
	}

	return s.obj.Object()
}

func (c *clientCacheWrapper) get(key *ecdsa.PrivateKey, addr string) (getClient, error) {
	clt, err := c.cache.Get(key, addr, c.opts...)

	return &clientWrapper{
		client: clt,
	}, err
}

func (c *clientWrapper) GetObject(ctx context.Context, p Prm) (*objectSDK.Object, error) {
	// we don't specify payload writer because we accumulate
	// the object locally (even huge).
	return c.client.GetObject(ctx,
		new(client.GetObjectParams).
			WithAddress(p.addr).
			WithRawFlag(true),
		p.callOpts...,
	)
}

func (e *storageEngineWrapper) Get(addr *objectSDK.Address) (*object.Object, error) {
	r, err := e.engine.Get(new(engine.GetPrm).
		WithAddress(addr),
	)
	if err != nil {
		return nil, err
	}

	return r.Object(), nil
}

func (s *headSvcWrapper) head(ctx context.Context, p Prm) (*object.Object, error) {
	r, err := s.svc.Head(ctx, new(headsvc.Prm).
		WithAddress(p.addr).
		WithCommonPrm(p.common).
		Short(false),
	)

	if err != nil {
		return nil, err
	}

	return r.Header(), nil
}
