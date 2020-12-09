package getsvc

import (
	"crypto/ecdsa"
	"hash"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

// Prm groups parameters of Get service call.
type Prm struct {
	commonPrm
}

// RangePrm groups parameters of GetRange service call.
type RangePrm struct {
	commonPrm

	rng *objectSDK.Range
}

// RangeHashPrm groups parameters of GetRange service call.
type RangeHashPrm struct {
	commonPrm

	hashGen func() hash.Hash

	rngs []*objectSDK.Range
}

// HeadPrm groups parameters of Head service call.
type HeadPrm struct {
	commonPrm
}

type commonPrm struct {
	objWriter ObjectWriter

	// TODO: replace key and callOpts to CommonPrm
	key *ecdsa.PrivateKey

	callOpts []client.CallOption

	common *util.CommonPrm

	client.GetObjectParams
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

// SetPrivateKey sets private key to use during execution.
func (p *commonPrm) SetPrivateKey(key *ecdsa.PrivateKey) {
	p.key = key
}

// SetRemoteCallOptions sets call options remote remote client calls.
func (p *commonPrm) SetRemoteCallOptions(opts ...client.CallOption) {
	p.callOpts = opts
}

// SetChunkWriter sets target component to write the object payload range.
func (p *RangePrm) SetChunkWriter(w ChunkWriter) {
	p.objWriter = &partWriter{
		chunkWriter: w,
	}
}

// SetRange sets range of the requested payload data.
func (p *RangePrm) SetRange(rng *objectSDK.Range) {
	p.rng = rng
}

// SetRangeList sets list of object payload ranges.
func (p *RangeHashPrm) SetRangeList(rngs []*objectSDK.Range) {
	p.rngs = rngs
}

// SetHashGenerator sets constructor of hashing algorithm.
func (p *RangeHashPrm) SetHashGenerator(v func() hash.Hash) {
	p.hashGen = v
}

// SetCommonParameters sets common parameters of the operation.
func (p *commonPrm) SetCommonParameters(common *util.CommonPrm) {
	p.common = common
}

// SetHeaderWriter sets target component to write the object header.
func (p *HeadPrm) SetHeaderWriter(w HeaderWriter) {
	p.objWriter = &partWriter{
		headWriter: w,
	}
}
