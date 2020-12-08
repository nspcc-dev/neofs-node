package getsvc

import (
	"context"
	"crypto/ecdsa"
	"hash"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
)

type simpleObjectWriter struct {
	obj *object.RawObject

	pld []byte
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

type rangeWriter struct {
	ObjectWriter

	chunkWriter ChunkWriter
}

type hasherWrapper struct {
	hash hash.Hash
}

func newSimpleObjectWriter() *simpleObjectWriter {
	return &simpleObjectWriter{
		obj: object.NewRaw(),
	}
}

func (s *simpleObjectWriter) WriteHeader(obj *object.Object) error {
	s.obj = object.NewRawFromObject(obj)

	s.pld = make([]byte, 0, obj.PayloadSize())

	return nil
}

func (s *simpleObjectWriter) WriteChunk(p []byte) error {
	s.pld = append(s.pld, p...)
	return nil
}

func (s *simpleObjectWriter) Close() error {
	return nil
}

func (s *simpleObjectWriter) object() *object.Object {
	if len(s.pld) > 0 {
		s.obj.SetPayload(s.pld)
	}

	return s.obj.Object()
}

func (c *clientCacheWrapper) get(key *ecdsa.PrivateKey, addr string) (getClient, error) {
	clt, err := c.cache.Get(key, addr, c.opts...)

	return &clientWrapper{
		client: clt,
	}, err
}

func (c *clientWrapper) GetObject(ctx context.Context, p RangePrm) (*objectSDK.Object, error) {
	// we don't specify payload writer because we accumulate
	// the object locally (even huge).
	if p.rng != nil {
		data, err := c.client.ObjectPayloadRangeData(ctx,
			new(client.RangeDataParams).
				WithAddress(p.Address()).
				WithRange(p.rng).
				WithRaw(p.RawFlag()),
			p.callOpts...,
		)
		if err != nil {
			return nil, err
		}

		return payloadOnlyObject(data), nil
	} else {
		// we don't specify payload writer because we accumulate
		// the object locally (even huge).
		return c.client.GetObject(ctx,
			new(client.GetObjectParams).
				WithAddress(p.Address()).
				WithRawFlag(p.RawFlag()),
			p.callOpts...,
		)
	}
}

func (e *storageEngineWrapper) Get(p RangePrm) (*object.Object, error) {
	if p.rng != nil {
		r, err := e.engine.GetRange(new(engine.RngPrm).
			WithAddress(p.Address()).
			WithPayloadRange(p.rng),
		)
		if err != nil {
			return nil, err
		}

		return r.Object(), nil
	} else {
		r, err := e.engine.Get(new(engine.GetPrm).
			WithAddress(p.Address()),
		)
		if err != nil {
			return nil, err
		}

		return r.Object(), nil
	}
}

func (s *headSvcWrapper) head(ctx context.Context, p Prm) (*object.Object, error) {
	r, err := s.svc.Head(ctx, new(headsvc.Prm).
		WithAddress(p.Address()).
		WithCommonPrm(p.common).
		Short(false),
	)

	if err != nil {
		return nil, err
	}

	return r.Header(), nil
}

func (w *rangeWriter) WriteChunk(p []byte) error {
	return w.chunkWriter.WriteChunk(p)
}

func payloadOnlyObject(payload []byte) *objectSDK.Object {
	rawObj := object.NewRaw()
	rawObj.SetPayload(payload)

	return rawObj.Object().SDK()
}

func (h *hasherWrapper) WriteChunk(p []byte) error {
	_, err := h.hash.Write(p)
	return err
}
