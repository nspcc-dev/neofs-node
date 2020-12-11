package deletesvc

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

// Prm groups parameters of Delete service call.
type Prm struct {
	key *ecdsa.PrivateKey

	common *util.CommonPrm

	callOpts []client.CallOption

	client.DeleteObjectParams
}

// SetCommonParameters sets common parameters of the operation.
func (p *Prm) SetCommonParameters(common *util.CommonPrm) {
	p.common = common
}

// SetPrivateKey sets private key to use during execution.
func (p *Prm) SetPrivateKey(key *ecdsa.PrivateKey) {
	p.key = key
}

// SetRemoteCallOptions sets call options of remote client calls.
func (p *Prm) SetRemoteCallOptions(opts ...client.CallOption) {
	p.callOpts = opts
}
