package getsvc

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
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

func (c *clientWrapper) getObject(exec *execCtx, info coreclient.NodeInfo) (*object.Object, io.ReadCloser, error) {
	if exec.isForwardingEnabled() {
		obj, err := exec.prm.forwarder(exec.ctx, info, c.client)
		if err != nil {
			return nil, nil, err
		}
		return obj, nil, nil
	}

	key, err := exec.key()
	if err != nil {
		return nil, nil, err
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
			return nil, nil, err
		}

		return res.Header(), nil, nil
	}

	if rngH := exec.prmRangeHash; rngH != nil && exec.isRangeHashForwardingEnabled() {
		exec.prmRangeHash.forwardedRangeHashResponse, err = exec.prm.rangeForwarder(exec.ctx, info, c.client)
		return nil, nil, err
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
				obj, reader, err := c.get(exec, key)
				if err != nil {
					return nil, nil, err
				}
				defer func() { _ = reader.Close() }()

				pLen := obj.PayloadSize()
				from := rng.GetOffset()
				ln := rng.GetLength()
				var to uint64
				if ln != 0 {
					to = from + ln
				} else {
					to = pLen
				}

				if to < from || pLen < from || pLen < to {
					return nil, nil, logicerr.Wrap(apistatus.ErrObjectOutOfRange)
				}

				if from > 0 {
					_, err = io.CopyN(io.Discard, reader, int64(from))
					if err != nil {
						return nil, nil, fmt.Errorf("discard %d bytes in stream: %w", from, err)
					}
				}

				payload := make([]byte, to-from)
				_, err = io.ReadFull(reader, payload)
				if err != nil {
					return nil, nil, fmt.Errorf("read %d bytes from stream: %w", ln, err)
				}

				rangeObj := payloadOnlyObject(payload)
				return rangeObj, nil, nil
			}
			return nil, nil, err
		}

		rangeObj := payloadOnlyObject(res.PayloadRange())
		return rangeObj, nil, nil
	}

	return c.get(exec, key)
}

func (c *clientWrapper) get(exec *execCtx, key *ecdsa.PrivateKey) (*object.Object, io.ReadCloser, error) {
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
		return nil, nil, err
	}
	return res.Object(), res.Reader(), nil
}

func (e *storageEngineWrapper) get(exec *execCtx) (*object.Object, io.ReadCloser, error) {
	if exec.headOnly() {
		r, err := e.engine.Head(exec.address(), exec.isRaw())
		if err != nil {
			return nil, nil, err
		}

		return r, nil, nil
	}

	if rng := exec.ctxRange(); rng != nil {
		r, err := e.engine.GetRange(exec.address(), rng.GetOffset(), rng.GetLength())
		if err != nil {
			return nil, nil, err
		}

		o := object.New()
		o.SetPayload(r)

		return o, nil, nil
	}

	return e.engine.GetStream(exec.address())
}

func (w *partWriter) WriteChunk(p []byte) error {
	return w.chunkWriter.WriteChunk(p)
}

func (w *partWriter) WriteHeader(o *object.Object) error {
	// Range responses use only ChunkWriter and didn't emit headers.
	// Treat header writes as a no-op when no header writer is set so callers can
	// still "trigger" header-dependent flows without affecting the client stream.
	if w.headWriter == nil {
		return nil
	}
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

type StreamingWriter struct {
	obj *object.Object

	reader io.ReadCloser
	writer io.WriteCloser

	headerReceived chan struct{}
}

func NewStreamingWriter() *StreamingWriter {
	r, w := io.Pipe()
	return &StreamingWriter{
		obj:            object.New(),
		reader:         r,
		writer:         w,
		headerReceived: make(chan struct{}),
	}
}

func (s *StreamingWriter) WriteHeader(obj *object.Object) error {
	s.obj = obj
	close(s.headerReceived)
	return nil
}

func (s *StreamingWriter) WriteChunk(p []byte) error {
	_, err := s.writer.Write(p)
	return err
}

func (s *StreamingWriter) Object() *object.Object {
	return s.obj
}

func (s *StreamingWriter) Reader() io.ReadCloser {
	return s.reader
}

func (s *StreamingWriter) HeaderChannel() <-chan struct{} {
	return s.headerReceived
}

func (s *StreamingWriter) Close() error {
	return s.writer.Close()
}
