package getsvc

import (
	"io"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	internal "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

type SimpleObjectWriter struct {
	obj *object.Object

	pld []byte
}

type clientCacheWrapper struct {
	cache ClientConstructor
}

type clientWrapper struct {
	client coreclient.MultiAddressClient
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
		obj: object.New(),
	}
}

func (s *SimpleObjectWriter) WriteHeader(obj *object.Object) error {
	s.obj = obj

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

	return s.obj
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

func (c *clientWrapper) getObject(exec *execCtx, info coreclient.NodeInfo) (*object.Object, error) {
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
		var headPrm engine.HeadPrm
		headPrm.WithAddress(exec.address())
		headPrm.WithRaw(exec.isRaw())

		r, err := e.engine.Head(headPrm)
		if err != nil {
			return nil, err
		}

		return r.Header(), nil
	} else if rng := exec.ctxRange(); rng != nil {
		var getRange engine.RngPrm
		getRange.WithAddress(exec.address())
		getRange.WithPayloadRange(rng)

		r, err := e.engine.GetRange(getRange)
		if err != nil {
			return nil, err
		}

		return r.Object(), nil
	} else {
		var getPrm engine.GetPrm
		getPrm.WithAddress(exec.address())

		r, err := e.engine.Get(getPrm)
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

func payloadOnlyObject(payload []byte) *object.Object {
	obj := object.New()
	obj.SetPayload(payload)

	return obj
}

func (h *hasherWrapper) WriteChunk(p []byte) error {
	_, err := h.hash.Write(p)
	return err
}

func (n *nmSrcWrapper) currentEpoch() (uint64, error) {
	return n.nmSrc.Epoch()
}
