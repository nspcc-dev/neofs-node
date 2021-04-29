package searchsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

// Prm groups parameters of Get service call.
type Prm struct {
	writer IDListWriter

	common *util.CommonPrm

	client.SearchObjectParams

	forwarder RequestForwarder
}

// IDListWriter is an interface of target component
// to write list of object identifiers.
type IDListWriter interface {
	WriteIDs([]*objectSDK.ID) error
}

// RequestForwarder is a callback for forwarding of the
// original Search requests.
type RequestForwarder func(client.Client) ([]*objectSDK.ID, error)

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
