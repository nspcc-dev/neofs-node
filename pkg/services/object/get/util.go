package getsvc

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-sdk-go/bearer"
	"github.com/nspcc-dev/neofs-sdk-go/client"
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

type objectReadAuthPrm interface {
	WithinSession(session.Object)
	WithinSessionV2(sessionv2.Token)
	WithBearerToken(bearer.Token)
}

func applyObjectReadAuth(exec *execCtx, addr oid.Address, opts objectReadAuthPrm) {
	if stV2 := exec.prm.common.SessionTokenV2(); stV2 != nil {
		verb := sessionv2.VerbObjectGet
		if stV2.AssertVerb(verb, addr.Container()) {
			opts.WithinSessionV2(*stV2)
		}
	} else if st := exec.prm.common.SessionToken(); st != nil && st.AssertObject(addr.Object()) {
		opts.WithinSession(*st)
	}
	if bt := exec.prm.common.BearerToken(); bt != nil {
		opts.WithBearerToken(*bt)
	}
}

func objectGetOptions(exec *execCtx) client.PrmObjectGet {
	var opts client.PrmObjectGet
	if exec.prm.common.TTL() < 2 {
		opts.MarkLocal()
	}
	opts.WithXHeaders(exec.prm.common.XHeaders()...)
	if exec.isRaw() {
		opts.MarkRaw()
	}
	return opts
}

type partWriter struct {
	ObjectWriter

	headWriter internal.HeaderWriter

	chunkWriter ChunkWriter
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
	if exec.hasPayloadRange() {
		addr := exec.address()
		id := addr.Object()

		opts := objectGetOptions(exec)
		applyObjectReadAuth(exec, addr, &opts)
		first, second := exec.payloadRange.First, exec.payloadRange.Second
		switch exec.payloadRange.Mode {
		case common.PayloadRangeModeNone:
			panic("missing payload range")
		case common.PayloadRangeModeOffsetLength:
			opts.SetRange(first, second)
		case common.PayloadRangeModeBounds:
			opts.SetRangeBounds(first, second)
		case common.PayloadRangeModeFrom:
			opts.SetRangeFrom(first)
		case common.PayloadRangeModeSuffix:
			opts.SetRangeSuffix(first)
		}

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

	opts := objectGetOptions(exec)
	if exec.payloadOnly && !exec.hasPayloadRange() && !exec.recheckEACL {
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

	if exec.localGetBuffer != nil {
		n, stream, err := e.engine.ReadObject(ctx, exec.address(), exec.payloadRange, exec.localGetBuffer, exec.interceptLocalHeaderBinaryFn)
		if err == nil {
			exec.submitLocalGetStreamFn(n, stream)
		}
		return nil, nil, err
	}

	if exec.hasPayloadRange() {
		hdr, stream, err := e.engine.GetRangeStream(ctx, exec.address(), exec.payloadRange, true)
		if err != nil {
			return nil, stream, err
		}
		return hdr, stream, nil
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

func (w *directChildWriter) ValidateHeader(obj *object.Object) error {
	w.hdr = obj
	return nil
}

func (c *clientCacheWrapper) InitGetObjectStream(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey,
	cnr cid.ID, id oid.ID, local, verifyID bool, rng *object.Range, xs []string) (object.Object, io.ReadCloser, error) {
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
	if rng != nil {
		opts.SetRange(rng.GetOffset(), rng.GetLength())
		opts.MarkPayloadOnly()
	}

	hdr, rc, err := conn.ObjectGetInit(ctx, cnr, id, user.NewAutoIDSigner(pk), opts)
	if err != nil {
		return object.Object{}, nil, err
	}

	// TODO: SkipChecksumVerification() turns off checking all object checksums. Better to keep checking
	//  OID against header and payload checksum.

	if rng != nil {
		b := []byte{0}
		if _, err = io.ReadFull(rc, b); err != nil {
			return object.Object{}, nil, err
		}

		return object.Object{}, struct {
			io.Reader
			io.Closer
		}{
			Reader: io.MultiReader(bytes.NewReader(b), rc),
			Closer: rc,
		}, nil
	}

	return hdr, rc, nil
}

func (c *clientCacheWrapper) Head(ctx context.Context, node netmap.NodeInfo, pk ecdsa.PrivateKey, cnr cid.ID, id oid.ID) (object.Object, error) {
	conn, err := c.connect(ctx, node)
	if err != nil {
		return object.Object{}, err
	}

	var opts client.PrmObjectHead
	opts.MarkLocal()

	hdr, err := conn.ObjectHead(ctx, cnr, id, user.NewAutoIDSigner(pk), opts)
	if err != nil {
		return object.Object{}, fmt.Errorf("call HEAD API: %w", err)
	}

	return *hdr, nil
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
