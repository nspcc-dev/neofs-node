package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

type AddPeer struct {
	node []byte
}

// MorphEvent implements Neo:Morph Event interface.
func (AddPeer) MorphEvent() {}

func (s AddPeer) Node() []byte {
	return s.node
}

func ParseAddPeer(prms []smartcontract.Parameter) (event.Event, error) {
	var (
		ev  AddPeer
		err error
	)

	if ln := len(prms); ln != 1 {
		return nil, event.WrongNumberOfParameters(1, ln)
	}

	ev.node, err = client.BytesFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get integer epoch number")
	}

	return ev, nil
}
