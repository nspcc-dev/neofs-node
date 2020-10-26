package neofs

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

// Deposit structure of neofs.Deposit notification from mainnet chain.
type Deposit struct {
	id     []byte
	amount int64 // Fixed8
	from   util.Uint160
	to     util.Uint160
}

// MorphEvent implements Neo:Morph Event interface.
func (Deposit) MorphEvent() {}

// ID is a deposit transaction hash.
func (d Deposit) ID() []byte { return d.id }

// From is a script hash of asset sender in main net.
func (d Deposit) From() util.Uint160 { return d.from }

// To is a script hash of asset receiver in balance contract.
func (d Deposit) To() util.Uint160 { return d.to }

// Amount of transferred assets.
func (d Deposit) Amount() int64 { return d.amount }

// ParseDeposit notification into deposit structure.
func ParseDeposit(params []stackitem.Item) (event.Event, error) {
	var ev Deposit

	if ln := len(params); ln != 4 {
		return nil, event.WrongNumberOfParameters(4, ln)
	}

	// parse from
	from, err := client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get deposit sender")
	}

	ev.from, err = util.Uint160DecodeBytesBE(from)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert deposit sender to uint160")
	}

	// parse amount
	ev.amount, err = client.IntFromStackItem(params[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get deposit amount")
	}

	// parse to
	to, err := client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, errors.Wrap(err, "could not get deposit receiver")
	}

	ev.to, err = util.Uint160DecodeBytesBE(to)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert deposit receiver to uint160")
	}

	// parse id
	ev.id, err = client.BytesFromStackItem(params[3])
	if err != nil {
		return nil, errors.Wrap(err, "could not get deposit id")
	}

	return ev, nil
}
