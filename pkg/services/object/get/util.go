package getsvc

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	internalclient "github.com/nspcc-dev/neofs-node/pkg/services/object/internal/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

	headWriter internal.HeaderWriter

	chunkWriter ChunkWriter
}

type hasherWrapper struct {
	hash io.Writer
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

func (c *clientWrapper) getObject(exec *execCtx) (*object.Object, error) {
	if exec.isForwardingEnabled() {
		return exec.prm.forwarder(exec.ctx, c.client)
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

	if rngH := exec.prmRangeHash; rngH != nil && exec.isRangeHashForwardingEnabled() {
		exec.prmRangeHash.forwardedRangeHashResponse, err = exec.prm.rangeForwarder(exec.ctx, c.client)
		return nil, err
	}

	// we don't specify payload writer because we accumulate
	// the object locally (even huge).
	if rng := exec.ctxRange(); rng != nil {
		var prm internalclient.PayloadRangePrm

		prm.SetContext(exec.context())
		prm.SetClient(c.client)
		prm.SetTTL(exec.prm.common.TTL())
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
			if errors.Is(err, apistatus.ErrObjectAccessDenied) {
				// Current spec allows other storage node to deny access,
				// fallback to GET here.
				obj, err := c.get(exec, key)
				if err != nil {
					return nil, err
				}

				payload := obj.Payload()
				from := rng.GetOffset()
				ln := rng.GetLength()
				if ln == 0 {
					ln = obj.PayloadSize()
				}
				to := from + ln

				if pLen := uint64(len(payload)); to < from || pLen < from || pLen < to {
					return nil, new(apistatus.ObjectOutOfRange)
				}

				return payloadOnlyObject(payload[from:to]), nil
			}
			return nil, err
		}

		return payloadOnlyObject(res.PayloadRange()), nil
	}

	return c.get(exec, key)
}

func (c *clientWrapper) get(exec *execCtx, key *ecdsa.PrivateKey) (*object.Object, error) {
	var prm internalclient.GetObjectPrm

	prm.SetContext(exec.context())
	prm.SetClient(c.client)
	prm.SetTTL(exec.prm.common.TTL())
	prm.SetAddress(exec.address())
	prm.SetPrivateKey(key)
	prm.SetSessionToken(exec.prm.common.SessionToken())
	prm.SetBearerToken(exec.prm.common.BearerToken())
	prm.SetXHeaders(exec.prm.common.XHeaders())

	if exec.isRaw() {
		prm.SetRawFlag()
	}

	res, err := internalclient.GetObject(prm)
	if err != nil {
		return nil, err
	}

	return res.Object(), nil
}

func (e *storageEngineWrapper) get(exec *execCtx) (*object.Object, error) {
	if exec.headOnly() {
		r, err := e.engine.Head(exec.address(), exec.isRaw())
		if err != nil {
			return nil, err
		}

		return r, nil
	}

	if rng := exec.ctxRange(); rng != nil {
		r, err := e.engine.GetRange(exec.address(), rng.GetOffset(), rng.GetLength())
		if err != nil {
			return nil, err
		}

		o := object.New()
		o.SetPayload(r)

		return o, nil
	}

	return e.engine.Get(exec.address())
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

func prettyRange(rng *object.Range) string {
	return fmt.Sprintf("[%d:%d]", rng.GetOffset(), rng.GetLength())
}
