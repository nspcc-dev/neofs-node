package getsvc

import (
	"crypto/ecdsa"
	"errors"
	"hash"

	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Prm groups parameters of Get service call.
type Prm struct {
	commonPrm
}

// RangePrm groups parameters of GetRange service call.
type RangePrm struct {
	commonPrm

	rng *object.Range
}

var (
	errRangeZeroLength = errors.New("zero range length")
	errRangeOverflow   = errors.New("range overflow")
)

// Validate pre-validates `OBJECTRANGE` request's parameters content
// without access to the requested object's payload.
func (p RangePrm) Validate() error {
	if p.rng != nil {
		off := p.rng.GetOffset()
		l := p.rng.GetLength()

		if l == 0 {
			return errRangeZeroLength
		}

		if off+l <= off {
			return errRangeOverflow
		}
	}

	return nil
}

// RangeHashPrm groups parameters of GetRange service call.
type RangeHashPrm struct {
	commonPrm

	hashGen func() hash.Hash

	rngs []object.Range

	salt []byte
}

type RequestForwarder func(coreclient.NodeInfo, coreclient.MultiAddressClient) (*object.Object, error)

// HeadPrm groups parameters of Head service call.
type HeadPrm struct {
	commonPrm
}

type commonPrm struct {
	objWriter ObjectWriter

	common *util.CommonPrm

	addr oid.Address

	raw bool

	forwarder RequestForwarder

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

// HeaderWriter is an interface of target component
// to write object header.
type HeaderWriter interface {
	WriteHeader(*object.Object) error
}

// ObjectWriter is an interface of target component to write object.
type ObjectWriter interface {
	HeaderWriter
	ChunkWriter
}

// SetObjectWriter sets target component to write the object.
func (p *Prm) SetObjectWriter(w ObjectWriter) {
	p.objWriter = w
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

// SetRangeList sets a list of object payload ranges.
func (p *RangeHashPrm) SetRangeList(rngs []object.Range) {
	p.rngs = rngs
}

// SetHashGenerator sets constructor of hashing algorithm.
func (p *RangeHashPrm) SetHashGenerator(v func() hash.Hash) {
	p.hashGen = v
}

// SetSalt sets binary salt to XOR object's payload ranges before hash calculation.
func (p *RangeHashPrm) SetSalt(salt []byte) {
	p.salt = salt
}

// SetCommonParameters sets common parameters of the operation.
func (p *commonPrm) SetCommonParameters(common *util.CommonPrm) {
	p.common = common
}

func (p *commonPrm) SetRequestForwarder(f RequestForwarder) {
	p.forwarder = f
}

// WithAddress sets object address to be read.
func (p *commonPrm) WithAddress(addr oid.Address) {
	p.addr = addr
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
func (p *HeadPrm) SetHeaderWriter(w HeaderWriter) {
	p.objWriter = &partWriter{
		headWriter: w,
	}
}
