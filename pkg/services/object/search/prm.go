package searchsvc

import (
	coreclient "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Prm groups parameters of Get service call.
type Prm struct {
	writer IDListWriter

	common *util.CommonPrm

	cnr cid.ID

	filters object.SearchFilters

	forwarder RequestForwarder
}

// IDListWriter is an interface of target component
// to write list of object identifiers.
type IDListWriter interface {
	WriteIDs([]oid.ID) error
}

// RequestForwarder is a callback for forwarding of the
// original Search requests.
type RequestForwarder func(coreclient.MultiAddressClient) ([]oid.ID, error)

// SetCommonParameters sets common parameters of the operation.
func (p *Prm) SetCommonParameters(common *util.CommonPrm) {
	p.common = common
}

// SetWriter sets target component to write list of object identifiers.
func (p *Prm) SetWriter(w IDListWriter) {
	p.writer = w
}

// SetRequestForwarder sets callback for forwarding
// of the original request.
func (p *Prm) SetRequestForwarder(f RequestForwarder) {
	p.forwarder = f
}

// WithContainerID sets identifier of the container to search the objects.
func (p *Prm) WithContainerID(id cid.ID) {
	p.cnr = id
}

// WithSearchFilters sets search filters.
func (p *Prm) WithSearchFilters(fs object.SearchFilters) {
	p.filters = fs
}
