package searchsvc

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

// Prm groups parameters of Get service call.
type Prm struct {
	writer IDListWriter

	// TODO: replace key and callOpts to CommonPrm
	key *ecdsa.PrivateKey

	callOpts []client.CallOption

	common *util.CommonPrm

	client.SearchObjectParams
}

// IDListWriter is an interface of target component
// to write list of object identifiers.
type IDListWriter interface {
	WriteIDs([]*objectSDK.ID) error
}

// SetPrivateKey sets private key to use during execution.
func (p *Prm) SetPrivateKey(key *ecdsa.PrivateKey) {
	p.key = key
}

// SetRemoteCallOptions sets call options remote remote client calls.
func (p *Prm) SetRemoteCallOptions(opts ...client.CallOption) {
	p.callOpts = opts
}

// SetCommonParameters sets common parameters of the operation.
func (p *Prm) SetCommonParameters(common *util.CommonPrm) {
	p.common = common
}

// SetWriter sets target component to write list of object identifiers.
func (p *Prm) SetWriter(w IDListWriter) {
	p.writer = w
}
