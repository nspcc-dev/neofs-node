package deletesvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

// TombstoneAddressWriter is an interface of tombstone address setter.
type TombstoneAddressWriter interface {
	SetAddress(*object.Address)
}

// Prm groups parameters of Delete service call.
type Prm struct {
	common *util.CommonPrm

	addr *object.Address

	tombAddrWriter TombstoneAddressWriter
}

// SetCommonParameters sets common parameters of the operation.
func (p *Prm) SetCommonParameters(common *util.CommonPrm) {
	p.common = common
}

// WithAddress sets address of the object to be removed.
func (p *Prm) WithAddress(addr *object.Address) {
	p.addr = addr
}

// WithTombstoneAddressTarget sets tombstone address destination.
func (p *Prm) WithTombstoneAddressTarget(w TombstoneAddressWriter) {
	p.tombAddrWriter = w
}
