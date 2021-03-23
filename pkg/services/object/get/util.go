package getsvc

import (
	"io"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
)

type SimpleObjectWriter struct {
	obj *object.RawObject

	pld []byte
}

type clientCacheWrapper struct {
	cache ClientConstructor
}

type clientWrapper struct {
	client client.Client
}

type storageEngineWrapper struct {
	engine *engine.StorageEngine
}

type partWriter struct {
	ObjectWriter

	headWriter HeaderWriter

	chunkWriter ChunkWriter
}

type hasherWrapper struct {
	hash io.Writer
}

type nmSrcWrapper struct {
	nmSrc netmap.Source
}

func NewSimpleObjectWriter() *SimpleObjectWriter {
	return &SimpleObjectWriter{
		obj: object.NewRaw(),
	}
}

func (s *SimpleObjectWriter) WriteHeader(obj *object.Object) error {
	s.obj = object.NewRawFromObject(obj)

	s.pld = make([]byte, 0, obj.PayloadSize())

	return nil
}

func (s *SimpleObjectWriter) WriteChunk(p []byte) error {
	s.pld = append(s.pld, p...)
	return nil
}

func (s *SimpleObjectWriter) Object() *object.Object {
	if len(s.pld) > 0 {
		s.obj.SetPayload(s.pld)
	}

	return s.obj.Object()
}

func (c *clientCacheWrapper) get(addr string) (getClient, error) {
	clt, err := c.cache.Get(addr)

	return &clientWrapper{
		client: clt,
	}, err
}

func (c *clientWrapper) getObject(exec *execCtx) (*objectSDK.Object, error) {
	if exec.headOnly() {
		return c.client.GetObjectHeader(exec.context(),
			new(client.ObjectHeaderParams).
				WithAddress(exec.address()).
				WithRawFlag(exec.isRaw()),
			exec.callOptions()...,
		)
	}
	// we don't specify payload writer because we accumulate
	// the object locally (even huge).
	if rng := exec.ctxRange(); rng != nil {
		data, err := c.client.ObjectPayloadRangeData(exec.context(),
			new(client.RangeDataParams).
				WithAddress(exec.address()).
				WithRange(rng).
				WithRaw(exec.isRaw()),
			exec.callOptions()...,
		)
		if err != nil {
			return nil, err
		}

		return payloadOnlyObject(data), nil
	}

	return c.client.GetObject(exec.context(),
		exec.remotePrm(),
		exec.callOptions()...,
	)
}

func (e *storageEngineWrapper) get(exec *execCtx) (*object.Object, error) {
	if exec.headOnly() {
		r, err := e.engine.Head(new(engine.HeadPrm).
			WithAddress(exec.address()).
			WithRaw(exec.isRaw()),
		)
		if err != nil {
			return nil, err
		}

		return r.Header(), nil
	} else if rng := exec.ctxRange(); rng != nil {
		r, err := e.engine.GetRange(new(engine.RngPrm).
			WithAddress(exec.address()).
			WithPayloadRange(rng),
		)
		if err != nil {
			return nil, err
		}

		return r.Object(), nil
	} else {
		r, err := e.engine.Get(new(engine.GetPrm).
			WithAddress(exec.address()),
		)
		if err != nil {
			return nil, err
		}

		return r.Object(), nil
	}
}

func (w *partWriter) WriteChunk(p []byte) error {
	return w.chunkWriter.WriteChunk(p)
}

func (w *partWriter) WriteHeader(o *object.Object) error {
	return w.headWriter.WriteHeader(o)
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

func (n *nmSrcWrapper) currentEpoch() (uint64, error) {
	return n.nmSrc.Epoch()
}
