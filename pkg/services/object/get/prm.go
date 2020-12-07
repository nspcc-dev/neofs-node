package getsvc

import (
	"crypto/ecdsa"

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

// ObjectWriter is an interface of target component to write object.
type ObjectWriter interface {
	WriteHeader(*object.Object) error
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

// SetObjectWriter sets target component to write the object payload range.
func (p *RangePrm) SetChunkWriter(w ChunkWriter) {
	p.objWriter = &rangeWriter{
		chunkWriter: w,
	}
}

// SetRange sets range of the requested payload data.
func (p *RangePrm) SetRange(rng *objectSDK.Range) {
	p.rng = rng
}
