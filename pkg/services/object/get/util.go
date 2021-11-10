package getsvc

import (
	"io"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	internal "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

type SimpleObjectWriter struct {
	obj *object.RawObject

	pld []byte
}

type clientCacheWrapper struct {
	cache ClientConstructor
}

type clientWrapper struct {
	client coreclient.Client
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

func (c *clientCacheWrapper) get(info coreclient.NodeInfo) (getClient, error) {
	clt, err := c.cache.Get(info)
	if err != nil {
		return nil, err
	}

	return &clientWrapper{
		client: clt,
	}, nil
}

func (c *clientWrapper) getObject(exec *execCtx, info coreclient.NodeInfo) (*objectSDK.Object, error) {
	if exec.isForwardingEnabled() {
		return exec.prm.forwarder(info, c.client)
	}

	key, err := exec.key()
	if err != nil {
		return nil, err
	}

	if exec.headOnly() {
		var prm internalclient.HeadObjectPrm

		prm.SetContext(exec.context())
		prm.SetClient(c.client)
		prm.SetTTL(exec.prm.common.TTL())
		prm.SetNetmapEpoch(exec.curProcEpoch)
		prm.SetAddress(exec.address())
		prm.SetPrivateKey(key)
		prm.SetSessionToken(exec.prm.common.SessionToken())
		prm.SetBearerToken(exec.prm.common.BearerToken())
		prm.SetXHeaders(exec.prm.common.XHeaders())

		if exec.isRaw() {
			prm.SetRawFlag()
		}

		res, err := internalclient.HeadObject(prm)
		if err != nil {
			return nil, err
		}

		return res.Header(), nil
	}
	// we don't specify payload writer because we accumulate
	// the object locally (even huge).
	if rng := exec.ctxRange(); rng != nil {
		var prm internalclient.PayloadRangePrm

		prm.SetContext(exec.context())
		prm.SetClient(c.client)
		prm.SetTTL(exec.prm.common.TTL())
		prm.SetNetmapEpoch(exec.curProcEpoch)
		prm.SetAddress(exec.address())
		prm.SetPrivateKey(key)
		prm.SetSessionToken(exec.prm.common.SessionToken())
		prm.SetBearerToken(exec.prm.common.BearerToken())
		prm.SetXHeaders(exec.prm.common.XHeaders())
		prm.SetRange(rng)

		if exec.isRaw() {
			prm.SetRawFlag()
		}

		res, err := internalclient.PayloadRange(prm)
		if err != nil {
			return nil, err
		}

		return payloadOnlyObject(res.PayloadRange()), nil
	}

	var prm internalclient.GetObjectPrm

	prm.SetContext(exec.context())
	prm.SetClient(c.client)
	prm.SetTTL(exec.prm.common.TTL())
	prm.SetNetmapEpoch(exec.curProcEpoch)
	prm.SetAddress(exec.address())
	prm.SetPrivateKey(key)
	prm.SetSessionToken(exec.prm.common.SessionToken())
	prm.SetBearerToken(exec.prm.common.BearerToken())
	prm.SetXHeaders(exec.prm.common.XHeaders())

	if exec.isRaw() {
		prm.SetRawFlag()
	}

	res, err := internal.GetObject(prm)
	if err != nil {
		return nil, err
	}

	return res.Object(), nil
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
