package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// NewEpoch is a new epoch Neo:Morph event.
type NewEpoch struct {
	num uint64
}

// MorphEvent implements Neo:Morph Event interface.
func (NewEpoch) MorphEvent() {}

// EpochNumber returns new epoch number.
func (s NewEpoch) EpochNumber() uint64 {
	return s.num
}

// ParseNewEpoch is a parser of new epoch notification event.
//
// Result is type of NewEpoch.
func ParseNewEpoch(prms []stackitem.Item) (event.Event, error) {
	if ln := len(prms); ln != 1 {
		return nil, event.WrongNumberOfParameters(1, ln)
	}

	prmEpochNum, err := client.IntFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get integer epoch number: %w", err)
	}

	return NewEpoch{
		num: uint64(prmEpochNum),
	}, nil
}
