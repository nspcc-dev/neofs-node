package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
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

func ParseAddPeer(prms []stackitem.Item) (event.Event, error) {
	var (
		ev  AddPeer
		err error
	)

	if ln := len(prms); ln != expectedItemNumAddPeer {
		return nil, event.WrongNumberOfParameters(expectedItemNumAddPeer, ln)
	}

	ev.node, err = client.BytesFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get raw nodeinfo: %w", err)
	}

	return ev, nil
}
