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
	objWriter ObjectWriter

	// TODO: replace key and callOpts to CommonPrm
	key *ecdsa.PrivateKey

	callOpts []client.CallOption

	common *util.CommonPrm

	// TODO: use parameters from NeoFS SDK
	addr *objectSDK.Address

	raw bool
}

// ObjectWriter is an interface of target component to write object.
type ObjectWriter interface {
	WriteHeader(*object.Object) error
	WriteChunk([]byte) error
}

// SetObjectWriter sets target component to write the object.
func (p *Prm) SetObjectWriter(w ObjectWriter) {
	p.objWriter = w
}

// SetPrivateKey sets private key to use during execution.
func (p *Prm) SetPrivateKey(key *ecdsa.PrivateKey) {
	p.key = key
}

// SetRemoteCallOptions sets call options remote remote client calls.
func (p *Prm) SetRemoteCallOptions(opts ...client.CallOption) {
	p.callOpts = opts
}

// SetAddress sets address of the requested object.
func (p *Prm) SetAddress(addr *objectSDK.Address) {
	p.addr = addr
}

// SetRaw sets raw flag. If flag is set,
// object assembling will not be undertaken.
func (p *Prm) SetRaw(raw bool) {
	p.raw = raw
}
