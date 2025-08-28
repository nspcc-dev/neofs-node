package getsvc

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// maxInitialBufferSize is the maximum initial buffer size for GetRange result.
// We don't want to allocate a lot of space in advance because a query can
// fail with apistatus.ObjectOutOfRange status.
const maxInitialBufferSize = 1024 * 1024 // 1 MiB

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

func (c *clientWrapper) getObject(exec *execCtx, info coreclient.NodeInfo) (*object.Object, error) {
	if exec.isForwardingEnabled() {
		return exec.prm.forwarder(exec.ctx, info, c.client)
	}

	key, err := exec.key()
	if err != nil {
		return nil, err
	}

	if exec.headOnly() {
		addr := exec.address()
		id := addr.Object()

		var opts client.PrmObjectHead
		if exec.prm.common.TTL() < 2 {
			opts.MarkLocal()
		}
		if st := exec.prm.common.SessionToken(); st != nil && st.AssertObject(id) {
			opts.WithinSession(*st)
		}
		if bt := exec.prm.common.BearerToken(); bt != nil {
			opts.WithBearerToken(*bt)
		}
		opts.WithXHeaders(exec.prm.common.XHeaders()...)
		if exec.isRaw() {
			opts.MarkRaw()
		}

		hdr, err := c.client.ObjectHead(exec.context(), addr.Container(), id, user.NewAutoIDSigner(*key), opts)
		if err != nil {
			return nil, fmt.Errorf("read object header from NeoFS: %w", err)
		}

		return hdr, nil
	}

	if rngH := exec.prmRangeHash; rngH != nil && exec.isRangeHashForwardingEnabled() {
		exec.prmRangeHash.forwardedRangeHashResponse, err = exec.prm.rangeForwarder(exec.ctx, info, c.client)
		return nil, err
	}

	// we don't specify payload writer because we accumulate
	// the object locally (even huge).
	if rng := exec.ctxRange(); rng != nil {
		addr := exec.address()
		id := addr.Object()
		ln := rng.GetLength()

		var opts client.PrmObjectRange
		if exec.prm.common.TTL() < 2 {
			opts.MarkLocal()
		}
		if st := exec.prm.common.SessionToken(); st != nil && st.AssertObject(id) {
			opts.WithinSession(*st)
		}
		if bt := exec.prm.common.BearerToken(); bt != nil {
			opts.WithBearerToken(*bt)
		}
		opts.WithXHeaders(exec.prm.common.XHeaders()...)
		if exec.isRaw() {
			opts.MarkRaw()
		}

		rdr, err := c.client.ObjectRangeInit(exec.context(), addr.Container(), id, rng.GetOffset(), ln, user.NewAutoIDSigner(*key), opts)
		if err != nil {
			err = fmt.Errorf("init payload reading: %w", err)
		} else {
			if int64(ln) < 0 {
				// `CopyN` expects `int64`, this check ensures that the result is positive.
				// On practice this means that we can return incorrect results for objects
				// with size > 8_388 Petabytes, this will be fixed later with support for streaming.
				return nil, new(apistatus.ObjectOutOfRange)
			}

			bufInitLen := min(ln, maxInitialBufferSize)

			w := bytes.NewBuffer(make([]byte, bufInitLen))
			_, err = io.CopyN(w, rdr, int64(ln))
			if err == nil {
				return payloadOnlyObject(w.Bytes()), nil
			}
			err = fmt.Errorf("read payload: %w", err)
		}
		if !errors.Is(err, apistatus.ErrObjectAccessDenied) {
			return nil, err
		}
		// Current spec allows other storage node to deny access,
		// fallback to GET here.
		obj, err := c.get(exec, key)
		if err != nil {
			return nil, err
		}

		payload := obj.Payload()
		from := rng.GetOffset()
		if ln == 0 {
			ln = obj.PayloadSize()
		}
		to := from + ln

		if pLen := uint64(len(payload)); to < from || pLen < from || pLen < to {
			return nil, new(apistatus.ObjectOutOfRange)
		}

		return payloadOnlyObject(payload[from:to]), nil
	}

	return c.get(exec, key)
}

func (c *clientWrapper) get(exec *execCtx, key *ecdsa.PrivateKey) (*object.Object, error) {
	addr := exec.address()
	id := addr.Object()

	var opts client.PrmObjectGet
	if exec.prm.common.TTL() < 2 {
		opts.MarkLocal()
	}
	if st := exec.prm.common.SessionToken(); st != nil && st.AssertObject(id) {
		opts.WithinSession(*st)
	}
	if bt := exec.prm.common.BearerToken(); bt != nil {
		opts.WithBearerToken(*bt)
	}
	opts.WithXHeaders(exec.prm.common.XHeaders()...)
	if exec.isRaw() {
		opts.MarkRaw()
	}

	hdr, rdr, err := c.client.ObjectGetInit(exec.context(), addr.Container(), id, user.NewAutoIDSigner(*key), opts)
	if err != nil {
		return nil, fmt.Errorf("init object reading:: %w", err)
	}

	buf := make([]byte, hdr.PayloadSize())

	_, err = rdr.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	hdr.SetPayload(buf)

	return &hdr, nil
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

	header, reader, err := e.engine.GetStream(exec.address())
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()

	payload, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("can't read object payload: %w", err)
	}
	header.SetPayload(payload)
	return header, nil
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
