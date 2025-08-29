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
		return obj, nil, err
	}

	key, err := exec.key()
	if err != nil {
		return nil, nil, err
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
			return nil, nil, fmt.Errorf("read object header from NeoFS: %w", err)
		}

		return hdr, nil, nil
	}

	if rngH := exec.prmRangeHash; rngH != nil && exec.isRangeHashForwardingEnabled() {
		exec.prmRangeHash.forwardedRangeHashResponse, err = exec.prm.rangeForwarder(exec.ctx, info, c.client)
		return nil, nil, err
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
		if err == nil {
			return nil, rdr, nil
		}
		if !errors.Is(err, apistatus.ErrObjectAccessDenied) {
			return nil, nil, fmt.Errorf("init payload reading: %w", err)
		}
		// Current spec allows other storage node to deny access,
		// fallback to GET here.
		hdr, reader, err := c.get(exec, key)
		if err != nil {
			return nil, nil, err
		}

		pLen := hdr.PayloadSize()
		from := rng.GetOffset()
		var to uint64
		if ln != 0 {
			to = from + ln
		} else {
			to = pLen
		}

		if to < from || pLen < from || pLen < to {
			return nil, nil, apistatus.ErrObjectOutOfRange
		}

		if from > 0 {
			_, err = io.CopyN(io.Discard, reader, int64(from))
			if err != nil {
				return nil, nil, fmt.Errorf("discard %d bytes in stream: %w", from, err)
			}
		}

		return nil, struct {
			io.Reader
			io.Closer
		}{
			Reader: io.LimitReader(reader, int64(to-from)),
			Closer: reader,
		}, nil
	}

	return c.get(exec, key)
}

func (c *clientWrapper) get(exec *execCtx, key *ecdsa.PrivateKey) (*object.Object, io.ReadCloser, error) {
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
		return nil, nil, fmt.Errorf("init object reader: %w", err)
	}
	return &hdr, rdr, nil
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
		// TODO: use here GetRangeStream when it will be implemented
		return nil, io.NopCloser(bytes.NewReader(r)), err
	}

	return e.engine.GetStream(exec.address())
}

func (w *partWriter) WriteChunk(p []byte) error {
	return w.chunkWriter.WriteChunk(p)
}

func (w *partWriter) WriteHeader(o *object.Object) error {
	return w.headWriter.WriteHeader(o)
}

func (h *hasherWrapper) WriteChunk(p []byte) error {
	_, err := h.hash.Write(p)
	return err
}

func prettyRange(rng *object.Range) string {
	return fmt.Sprintf("[%d:%d]", rng.GetOffset(), rng.GetLength())
}

// directChildWriter streams child object payload directly into destination ChunkWriter
// while capturing the header.
type directChildWriter struct {
	hdr *object.Object
	ChunkWriter
}

func newDirectChildWriter(dest ChunkWriter) *directChildWriter {
	return &directChildWriter{
		ChunkWriter: dest,
	}
}

func (w *directChildWriter) WriteHeader(obj *object.Object) error {
	w.hdr = obj
	return nil
}
