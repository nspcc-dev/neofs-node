package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpc/response/result/subscriptions"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

type AddPeer struct {
	node []byte

	// For notary notifications only.
	// Contains raw transactions of notary request.
	notaryRequest *payload.P2PNotaryRequest
}

// MorphEvent implements Neo:Morph Event interface.
func (AddPeer) MorphEvent() {}

func (s AddPeer) Node() []byte {
	return s.node
}

// NotaryRequest returns raw notary request if notification
// was received via notary service. Otherwise, returns nil.
func (s AddPeer) NotaryRequest() *payload.P2PNotaryRequest {
	return s.notaryRequest
}

const expectedItemNumAddPeer = 1

func ParseAddPeer(e *subscriptions.NotificationEvent) (event.Event, error) {
	var (
		ev  AddPeer
		err error
	)

	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	if ln := len(params); ln != expectedItemNumAddPeer {
		return nil, event.WrongNumberOfParameters(expectedItemNumAddPeer, ln)
	}

	ev.node, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get raw nodeinfo: %w", err)
	}

	return ev, nil
}
