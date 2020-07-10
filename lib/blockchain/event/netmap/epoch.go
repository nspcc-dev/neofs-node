package netmap

import (
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/event"
	"github.com/nspcc-dev/neofs-node/lib/blockchain/goclient"
	"github.com/pkg/errors"
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
func ParseNewEpoch(prms []smartcontract.Parameter) (event.Event, error) {
	if ln := len(prms); ln != 1 {
		return nil, event.WrongNumberOfParameters(1, ln)
	}

	prmEpochNum, err := goclient.IntFromStackParameter(prms[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get integer epoch number")
	}

	return NewEpoch{
		num: uint64(prmEpochNum),
	}, nil
}
