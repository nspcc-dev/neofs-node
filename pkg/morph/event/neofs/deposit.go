package neofs

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
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
		return nil, fmt.Errorf("could not get deposit sender: %w", err)
	}

	ev.from, err = util.Uint160DecodeBytesBE(from)
	if err != nil {
		return nil, fmt.Errorf("could not convert deposit sender to uint160: %w", err)
	}

	// parse amount
	ev.amount, err = client.IntFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get deposit amount: %w", err)
	}

	// parse to
	to, err := client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, fmt.Errorf("could not get deposit receiver: %w", err)
	}

	ev.to, err = util.Uint160DecodeBytesBE(to)
	if err != nil {
		return nil, fmt.Errorf("could not convert deposit receiver to uint160: %w", err)
	}

	// parse id
	ev.id, err = client.BytesFromStackItem(params[3])
	if err != nil {
		return nil, fmt.Errorf("could not get deposit id: %w", err)
	}

	return ev, nil
}
