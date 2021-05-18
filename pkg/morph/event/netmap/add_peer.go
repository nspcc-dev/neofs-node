package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

type AddPeer struct {
	node []byte
}

// MorphEvent implements Neo:Morph Event interface.
func (AddPeer) MorphEvent() {}

func (s AddPeer) Node() []byte {
	return s.node
}

func ParseAddPeer(prms []stackitem.Item) (event.Event, error) {
	var (
		ev  AddPeer
		err error
	)

	if ln := len(prms); ln != 1 {
		return nil, event.WrongNumberOfParameters(1, ln)
	}

	ev.node, err = client.BytesFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get integer epoch number: %w", err)
	}

	return ev, nil
}
