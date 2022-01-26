package deletesvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

// TombstoneAddressWriter is an interface of tombstone address setter.
type TombstoneAddressWriter interface {
	SetAddress(*addressSDK.Address)
}

// Prm groups parameters of Delete service call.
type Prm struct {
	common *util.CommonPrm

	addr *addressSDK.Address

	tombAddrWriter TombstoneAddressWriter
}

// SetCommonParameters sets common parameters of the operation.
func (p *Prm) SetCommonParameters(common *util.CommonPrm) {
	p.common = common
}

// WithAddress sets address of the object to be removed.
func (p *Prm) WithAddress(addr *addressSDK.Address) {
	p.addr = addr
}

// WithTombstoneAddressTarget sets tombstone address destination.
func (p *Prm) WithTombstoneAddressTarget(w TombstoneAddressWriter) {
	p.tombAddrWriter = w
}
