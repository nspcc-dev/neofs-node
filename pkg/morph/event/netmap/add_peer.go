package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
)

// AddNode contains addNode method parameters.
type AddNode struct {
	Node netmaprpc.NetmapNode2

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (AddNode) MorphEvent() {}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (s AddNode) NotaryRequest() *payload.P2PNotaryRequest {
	return s.notaryRequest
}
