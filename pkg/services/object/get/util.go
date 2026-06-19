package getsvc

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// TODO: share. We also use stop error for BoltDB iterators and so on.
var errInterrupt = errors.New("interrupt")

var errInvalidSizeSplitLinker = errors.New("invalid size-split linker")

type sizeSplitinkerError object.Object

func (x sizeSplitinkerError) Error() string {
	return "object is size-split linker"
}

type SimpleObjectWriter struct {
	obj *object.Object

	pld []byte
}

type clientCacheWrapper struct {
	cache ClientConstructor
}

type clientWrapper struct {
	client clientcore.MultiAddressClient
}

type storageEngineWrapper struct {
	engine *engine.StorageEngine
}

type partWriter struct {
	ObjectWriter

	headWriter internal.HeaderWriter

	chunkWriter ChunkWriter
}

// fallbackRangeReader wraps a range reader obtained via ObjectRangeInit and
// falls back to a full GET in case apistatus.ErrObjectAccessDenied is
// returned while reading.
type fallbackRangeReader struct {
	io.ReadCloser
	exec   *execCtx
	client *clientWrapper
	key    *ecdsa.PrivateKey
	rng    *object.Range

	delivered       uint64
	fallbackPending bool
	fallbackDone    bool
}

func (exec execCtx) fallbackGetExec() execCtx {
	exec.prm.rng = nil
	exec.payloadOnly = false
	exec.legacyRange = false

	return exec
}

func newFallbackRangeReader(exec *execCtx, c *clientWrapper, key *ecdsa.PrivateKey, rng *object.Range, rdr io.ReadCloser) io.ReadCloser {
	return &fallbackRangeReader{
		ReadCloser: rdr,
		exec:       exec,
		client:     c,
		key:        key,
		rng:        rng,
	}
}

func (f *fallbackRangeReader) Read(p []byte) (int, error) {
	if f.fallbackPending && !f.fallbackDone {
		return f.fallbackRead(p)
	}

	n, err := f.ReadCloser.Read(p)
	f.delivered += uint64(n)
	if err == nil || !errors.Is(err, apistatus.ErrObjectAccessDenied) || f.fallbackDone {
		return n, err
	}
	if n > 0 {
		f.fallbackPending = true
		return n, nil
	}

	return f.fallbackRead(p)
}

func (f *fallbackRangeReader) fallbackBounds(payloadSize uint64) (from, to uint64, err error) {
	base := f.rng.GetOffset()
	from = base + f.delivered
	if from < base {
		return 0, 0, apistatus.ErrObjectOutOfRange
	}

	if ln := f.rng.GetLength(); ln != 0 {
		to = base + ln
		if to < base || to < from {
			return 0, 0, apistatus.ErrObjectOutOfRange
		}
	} else {
		to = payloadSize
	}

	if payloadSize < from || payloadSize < to {
		return 0, 0, apistatus.ErrObjectOutOfRange
	}

	return from, to, nil
}

func (f *fallbackRangeReader) fallbackRead(p []byte) (int, error) {
	// TODO: drop fallback once legacy RANGE is aligned with GET access semantics, see #3547.
	f.exec.log.Debug("range read access denied, falling back to full GET")
	f.fallbackPending = false
	f.fallbackDone = true

	oldRdr := f.ReadCloser
	if oldRdr != nil {
		defer func() { _ = oldRdr.Close() }()
	}

	fallbackExec := f.exec.fallbackGetExec()
	hdr, rdr, getErr := f.client.get(&fallbackExec, f.key)
	if getErr != nil {
		return 0, fmt.Errorf("fallback GET after access denial failed: %w", getErr)
	}

	from, to, err := f.fallbackBounds(hdr.PayloadSize())
	if err != nil {
		_ = rdr.Close()
		return 0, err
	}

	if from > 0 {
		_, err = io.CopyN(io.Discard, rdr, int64(from))
		if err != nil {
			_ = rdr.Close()
			return 0, fmt.Errorf("discard %d bytes in stream: %w", from, err)
		}
	}

	f.ReadCloser = struct {
		io.Reader
		io.Closer
	}{
		Reader: io.LimitReader(rdr, int64(to-from)),
		Closer: rdr,
	}

	// attempt to read again immediately to fill p.
	return f.Read(p)
}

func NewSimpleObjectWriter() *SimpleObjectWriter {
	return &SimpleObjectWriter{
		obj: new(object.Object),
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

func (c *clientCacheWrapper) get(ctx context.Context, info netmap.NodeInfo) (getClient, error) {
	clt, err := c.cache.Get(ctx, info)
	if err != nil {
		return nil, err
	}

	return &clientWrapper{
		client: clt,
	}, nil
}

func (c *clientWrapper) getObject(exec *execCtx) (*object.Object, io.ReadCloser, error) {
	if exec.headTransportFn != nil {
		respBuf, hdr, err := exec.headTransportFn(exec.ctx, c.client)
		if err == nil {
			exec.submitHeadResponseFn(respBuf, hdr)
		}
		return nil, nil, err
	}

	if exec.getTransportFn != nil {
		return nil, nil, exec.getTransportFn(exec.ctx, c.client)
	}

	if exec.rangeTransportFn != nil {
		return nil, nil, exec.rangeTransportFn(exec.ctx, c.client)
	}

	key, err := exec.key()
	if err != nil {
		return nil, nil, err
	}

	if exec.headOnly() {
		hdr, err := c.head(exec, key)
		if err != nil {
			return nil, nil, err
		}

		return hdr, nil, nil
	}

	// we don't specify payload writer because we accumulate
	// the object locally (even huge).
	if rng := exec.ctxRange(); rng != nil {
		addr := exec.address()
		id := addr.Object()

		if exec.legacyRange {
			ln := rng.GetLength()

			var opts client.PrmObjectRange
			if exec.prm.common.TTL() < 2 {
				opts.MarkLocal()
			}
			if stV2 := exec.prm.common.SessionTokenV2(); stV2 != nil {
				if stV2.AssertVerb(sessionv2.VerbObjectRange, addr.Container()) {
					opts.WithinSessionV2(*stV2)
				}
			} else if st := exec.prm.common.SessionToken(); st != nil && st.AssertObject(id) {
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
				return nil, nil, fmt.Errorf("init payload reading: %w", err)
			}

			hdr, err := c.head(exec, key)
			if err != nil {
				_ = rdr.Close()
				return nil, nil, err
			}

			return hdr, newFallbackRangeReader(exec, c, key, rng, rdr), nil
		}

		var opts client.PrmObjectGet
		if exec.prm.common.TTL() < 2 {
			opts.MarkLocal()
		}
		if stV2 := exec.prm.common.SessionTokenV2(); stV2 != nil {
			if stV2.AssertVerb(sessionv2.VerbObjectGet, addr.Container()) {
				opts.WithinSessionV2(*stV2)
			}
		} else if st := exec.prm.common.SessionToken(); st != nil && st.AssertObject(id) {
			opts.WithinSession(*st)
		}
		if bt := exec.prm.common.BearerToken(); bt != nil {
			opts.WithBearerToken(*bt)
		}
		opts.WithXHeaders(exec.prm.common.XHeaders()...)
		if exec.isRaw() {
			opts.MarkRaw()
		}
		opts.SetRange(rng.GetOffset(), rng.GetLength())

		hdr, rdr, err := c.client.ObjectGetInit(exec.context(), addr.Container(), id, user.NewAutoIDSigner(*key), opts)
		if err != nil {
			return nil, nil, fmt.Errorf("init payload reading: %w", err)
		}
		return &hdr, rdr, nil
	}

	return c.get(exec, key)
}

func (c *clientWrapper) head(exec *execCtx, key *ecdsa.PrivateKey) (*object.Object, error) {
	addr := exec.address()
	id := addr.Object()

	var opts client.PrmObjectHead
	if exec.prm.common.TTL() < 2 {
		opts.MarkLocal()
	}
	if stV2 := exec.prm.common.SessionTokenV2(); stV2 != nil {
		if stV2.AssertVerb(sessionv2.VerbObjectHead, addr.Container()) {
			opts.WithinSessionV2(*stV2)
		}
	} else if st := exec.prm.common.SessionToken(); st != nil && st.AssertObject(id) {
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

func (c *clientWrapper) get(exec *execCtx, key *ecdsa.PrivateKey) (*object.Object, io.ReadCloser, error) {
	addr := exec.address()
	id := addr.Object()

	var opts client.PrmObjectGet
	if exec.prm.common.TTL() < 2 {
		opts.MarkLocal()
	}
	if stV2 := exec.prm.common.SessionTokenV2(); stV2 != nil {
		if stV2.AssertVerb(sessionv2.VerbObjectGet, addr.Container()) {
			opts.WithinSessionV2(*stV2)
		}
	} else if st := exec.prm.common.SessionToken(); st != nil && st.AssertObject(id) {
		opts.WithinSession(*st)
	}
	if bt := exec.prm.common.BearerToken(); bt != nil {
		opts.WithBearerToken(*bt)
	}
	opts.WithXHeaders(exec.prm.common.XHeaders()...)
	if exec.isRaw() {
		opts.MarkRaw()
	}
	if exec.payloadOnly && exec.ctxRange() == nil && !exec.recheckEACL {
		opts.MarkPayloadOnly()
	}

	hdr, rdr, err := c.client.ObjectGetInit(exec.context(), addr.Container(), id, user.NewAutoIDSigner(*key), opts)
	if err != nil {
		return nil, nil, fmt.Errorf("init object reader: %w", err)
	}
	return &hdr, rdr, nil
}

func (e *storageEngineWrapper) get(exec *execCtx) (*object.Object, io.ReadCloser, error) {
	ctx := exec.context()
	if exec.headOnly() {
		r, err := e.engine.Head(ctx, exec.address(), exec.isRaw())
		if err != nil {
			return nil, nil, err
		}

		return r, nil, nil
	}

	if rng := exec.ctxRange(); rng != nil {
		if exec.localRangeBuffer != nil {
			r, err := e.engine.ReadPayloadRange(ctx, exec.address(), rng.GetOffset(), rng.GetLength(), exec.localRangeBuffer)
			if err == nil {
				exec.submitLocalRangeStreamFn(r)
			}
			return nil, nil, err
		}
		r, err := e.engine.GetRangeStream(ctx, exec.address(), rng.GetOffset(), rng.GetLength())
		if err != nil {
			return nil, r, err
		}
		// TODO: avoid extra local HEAD once we can get header with range stream from engine in one call.
		h, hErr := e.engine.Head(ctx, exec.address(), exec.isRaw())
		if hErr != nil {
			if r != nil {
				_ = r.Close()
			}
			return nil, nil, hErr
		}
		return h, r, nil
	}

	if exec.localGetBuffer != nil {
		n, stream, err := e.engine.ReadObject(ctx, exec.address(), exec.localGetBuffer)
		if err == nil {
			exec.submitLocalGetStreamFn(n, stream)
		}
		return nil, nil, err
	}

	return e.engine.GetStream(ctx, exec.address())
}

func (w *partWriter) WriteChunk(p []byte) error {
	return w.chunkWriter.WriteChunk(p)
}

func (w *partWriter) WriteHeader(o *object.Object) error {
	return w.headWriter.WriteHeader(o)
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

func (c *clientCacheWrapper) InitGetObjectStream(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey,
	cnr cid.ID, id oid.ID, sTok *session.Object, local, verifyID bool, xs []string) (object.Object, io.ReadCloser, error) {
	conn, err := c.connect(ctx, node)
	if err != nil {
		return object.Object{}, nil, err
	}

	var opts client.PrmObjectGet
	opts.WithXHeaders(xs...)
	if local {
		opts.MarkLocal()
	}
	if !verifyID {
		opts.SkipChecksumVerification()
	}
	if sTok != nil {
		opts.WithinSession(*sTok)
	}

	hdr, rc, err := conn.ObjectGetInit(ctx, cnr, id, user.NewAutoIDSigner(pk), opts)
	if err != nil {
		return object.Object{}, nil, err
	}

	// TODO: SkipChecksumVerification() turns off checking all object checksums. Better to keep checking
	//  OID against header and payload checksum.

	return hdr, rc, nil
}

func (c *clientCacheWrapper) Head(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID,
	sTok *session.Object) (object.Object, error) {
	conn, err := c.connect(ctx, node)
	if err != nil {
		return object.Object{}, err
	}

	var opts client.PrmObjectHead
	opts.MarkLocal()
	if sTok != nil {
		opts.WithinSession(*sTok)
	}

	hdr, err := conn.ObjectHead(ctx, cnr, id, user.NewAutoIDSigner(pk), opts)
	if err != nil {
		return object.Object{}, fmt.Errorf("call HEAD API: %w", err)
	}

	return *hdr, nil
}

func (c *clientCacheWrapper) InitGetObjectRangeStream(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey,
	cnr cid.ID, id oid.ID, off, ln uint64, sTok *session.Object, xs []string) (io.ReadCloser, error) {
	conn, err := c.connect(ctx, node)
	if err != nil {
		return nil, err
	}

	var opts client.PrmObjectRange
	opts.WithXHeaders(xs...)
	opts.MarkLocal()
	if sTok != nil {
		opts.WithinSession(*sTok)
	}

	rc, err := conn.ObjectRangeInit(ctx, cnr, id, off, ln, user.NewAutoIDSigner(pk), opts)
	if err != nil {
		return nil, fmt.Errorf("open GetRange stream: %w", err)
	}

	return rc, nil
}

func (c *clientCacheWrapper) connect(ctx context.Context, node netmap.NodeInfo) (clientcore.MultiAddressClient, error) {
	conn, err := c.cache.Get(ctx, node)
	if err != nil {
		return nil, fmt.Errorf("get conn: %w", err)
	}

	return conn, nil
}

// partialObjectCopy contains information about incomplete copying of some
// object.
type partialObjectCopy struct {
	// Whether header was copied or not.
	copiedHeader bool
	// Number of payload bytes copied.
	copiedPayloadLength uint64
}

// Error implements [error].
func (x partialObjectCopy) Error() string {
	return fmt.Sprintf("incomplete object copy (copied header: %t, copied payload: %d bytes)",
		x.copiedHeader, x.copiedPayloadLength)
}
