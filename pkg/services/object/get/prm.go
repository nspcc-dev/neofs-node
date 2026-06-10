package getsvc

import (
	"context"
	"crypto/ecdsa"
	"io"

	iprotobuf "github.com/nspcc-dev/neofs-node/internal/protobuf"
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/internal"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"google.golang.org/grpc/mem"
)

// SubmitStreamFunc is a callback for partially read object stream.
type SubmitStreamFunc = func(int, io.ReadCloser)

// SubmitDataStreamFunc is a handler of data stream.
type SubmitDataStreamFunc = func(io.ReadCloser)

// Prm groups parameters of Get service call.
type Prm struct {
	commonPrm

	rng         *object.Range
	payloadOnly bool

	localGetBuffer         []byte
	submitLocalGetStreamFn SubmitStreamFunc

	forwardRequestFn ForwardGetRequestFunc

	ecTransport GetECRequestTransport
}

// RangePrm groups parameters of GetRange service call.
type RangePrm struct {
	commonPrm

	rng *object.Range

	localBuffer         []byte
	submitLocalStreamFn SubmitDataStreamFunc

	forwardRequestFn ForwardRangeRequestFunc
}

type RequestForwarder func(context.Context, clientcore.MultiAddressClient) (*object.Object, error)

// ForwardHeadRequestFunc sends currently served HEAD request to remote node
// through passed connection and returns buffered response with requested
// object's header binary in it.
type ForwardHeadRequestFunc = func(context.Context, clientcore.MultiAddressClient) (mem.BufferSlice, iprotobuf.BuffersSlice, error)

// SubmitHeadResponseFunc accepts result of [ForwardHeadRequestFunc].
type SubmitHeadResponseFunc = func(mem.BufferSlice, iprotobuf.BuffersSlice)

// ForwardGetRequestFunc continues to serve current GET request from remote node
// through passed connection.
type ForwardGetRequestFunc = func(context.Context, clientcore.MultiAddressClient) error

// ForwardRangeRequestFunc continues to serve current RANGE request from remote node
// through passed connection.
type ForwardRangeRequestFunc = func(context.Context, clientcore.MultiAddressClient) error

// HeadPrm groups parameters of Head service call.
type HeadPrm struct {
	commonPrm

	buffer      []byte
	submitLenFn func(int)

	forwardHeadRequestFn ForwardHeadRequestFunc

	submitHeadResponseFn SubmitHeadResponseFunc
}

type commonPrm struct {
	objWriter ObjectWriter

	common *util.CommonPrm

	addr      oid.Address
	container container.Container

	raw bool

	// signerKey is a cached key that should be used for spawned
	// requests (if any), could be nil if incoming request handling
	// routine does not include any key fetching operations
	signerKey *ecdsa.PrivateKey
}

// ChunkWriter is an interface of target component
// to write payload chunk.
type ChunkWriter interface {
	WriteChunk([]byte) error
}

// ObjectWriter is an interface of target component to write object.
type ObjectWriter interface {
	internal.HeaderWriter
	ChunkWriter
}

// HeaderValidator is an optional interface for validating object headers
// before suppressing them from the response.
type HeaderValidator interface {
	ValidateHeader(*object.Object) error
}

// SetObjectWriter sets target component to write the object.
func (p *Prm) SetObjectWriter(w ObjectWriter) {
	p.objWriter = w
}

// SetRange sets range of the requested payload data.
func (p *Prm) SetRange(rng *object.Range) {
	p.rng = rng
}

// MarkPayloadOnly requests payload without an object header.
func (p *Prm) MarkPayloadOnly() {
	p.payloadOnly = true
}

// SetChunkWriter sets target component to write the object payload range.
func (p *RangePrm) SetChunkWriter(w ChunkWriter) {
	p.objWriter = &partWriter{
		chunkWriter: w,
	}
}

// SetRange sets range of the requested payload data.
func (p *RangePrm) SetRange(rng *object.Range) {
	p.rng = rng
}

// SetCommonParameters sets common parameters of the operation.
func (p *commonPrm) SetCommonParameters(common *util.CommonPrm) {
	p.common = common
}

// WithAddress sets object address to be read.
func (p *commonPrm) WithAddress(addr oid.Address) {
	p.addr = addr
}

// WithContainer sets container data to be used.
func (p *commonPrm) WithContainer(cnr container.Container) {
	p.container = cnr
}

// WithRawFlag sets flag of raw reading.
func (p *commonPrm) WithRawFlag(raw bool) {
	p.raw = raw
}

// WithCachedSignerKey sets optional key for all further requests.
func (p *commonPrm) WithCachedSignerKey(signerKey *ecdsa.PrivateKey) {
	p.signerKey = signerKey
}

// SetHeaderWriter sets target component to write the object header.
func (p *HeadPrm) SetHeaderWriter(w internal.HeaderWriter) {
	p.objWriter = &partWriter{
		headWriter: w,
	}
}

// WithBuffer specifies a buffer into which header of the requested object is
// optionally written. The submitLenFn parameter is a callback for number of
// bytes written. If buffer is unused, submitLenFn is not called.
func (p *HeadPrm) WithBuffer(buffer []byte, submitLenFn func(int)) {
	p.buffer = buffer
	p.submitLenFn = submitLenFn
}

// WithBuffer specifies a buffer into which header of the requested object is
// optionally written. The submitStreamFn parameter is a callback for number of
// bytes written and stream of remaining bytes. If buffer is unused,
// submitStreamFn is not called. The stream must be finally closed by the
// caller.
func (p *Prm) WithBuffer(buffer []byte, submitStreamFn SubmitStreamFunc) {
	p.localGetBuffer = buffer
	p.submitLocalGetStreamFn = submitStreamFn
}

// GetBuffer returns buffer settings set using [Prm.WithBuffer].
func (p Prm) GetBuffer() ([]byte, SubmitStreamFunc) {
	return p.localGetBuffer, p.submitLocalGetStreamFn
}

// Range returns payload range settings.
func (p Prm) Range() *object.Range {
	return p.rng
}

// PayloadOnly reports whether only payload was requested.
func (p Prm) PayloadOnly() bool {
	return p.payloadOnly
}

// SetRequestForwarder specifies request transport callback to use for receiving
// response from remote node.
//
// The f should return:
//   - response buffer and object header protobuf without an error on OK
//   - [object.SplitInfoError] on OK with corresponding body field
//   - [apistatus.ErrObjectNotFound] on 404 status
//   - (respBuf, iprotobuf.BuffersSlice{}, nil) on other API statuses
//   - any transport error
//
// Once results successfully received, it is forwarded untouched to handler
// which must be set via [HeadPrm.SetSubmitHeadResponseFunc].
func (p *HeadPrm) SetRequestForwarder(f ForwardHeadRequestFunc) {
	p.forwardHeadRequestFn = f
}

// SetSubmitHeadResponseFunc specifies handler to pass results of
// [HeadPrm.SetRequestForwarder] argument into.
func (p *HeadPrm) SetSubmitHeadResponseFunc(f SubmitHeadResponseFunc) {
	p.submitHeadResponseFn = f
}

// SetRequestForwarder specifies request transport callback to use for streaming
// responses from remote node.
//
// The f should return:
//   - nil on completed object transmission
//   - [apistatus.ErrObjectNotFound] on 404 status
//   - nil on other API statuses
//   - any other transport/protocol error otherwise
func (p *Prm) SetRequestForwarder(f ForwardGetRequestFunc) {
	p.forwardRequestFn = f
}

// WithBuffer specifies a buffer to use for header reading and a callback for
// payload range stream. If passed, the stream must be finally closed by the
// caller.
func (p *RangePrm) WithBuffer(buffer []byte, submitStreamFn SubmitDataStreamFunc) {
	p.localBuffer = buffer
	p.submitLocalStreamFn = submitStreamFn
}

// SetRequestForwarder specifies request transport callback to use for streaming
// responses from remote node.
//
// The f should return:
//   - nil on completed object transmission
//   - [object.SplitInfoError] on OK with corresponding body field
//   - [apistatus.ErrObjectNotFound] on 404 status
//   - nil on other API statuses
//   - any other transport/protocol error otherwise
func (p *RangePrm) SetRequestForwarder(f ForwardRangeRequestFunc) {
	p.forwardRequestFn = f
}

// WithECTransport specifies transport layer to for EC handling.
func (p *Prm) WithECTransport(transport GetECRequestTransport) {
	p.ecTransport = transport
}
