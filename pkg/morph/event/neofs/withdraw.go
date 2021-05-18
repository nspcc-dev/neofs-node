package neofs

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// Withdraw structure of neofs.Withdraw notification from mainnet chain.
type Withdraw struct {
	id     []byte
	amount int64 // Fixed8
	user   util.Uint160
}

// MorphEvent implements Neo:Morph Event interface.
func (Withdraw) MorphEvent() {}

// ID is a withdraw transaction hash.
func (w Withdraw) ID() []byte { return w.id }

// User returns withdraw receiver script hash from main net.
func (w Withdraw) User() util.Uint160 { return w.user }

// Amount of the withdraw assets.
func (w Withdraw) Amount() int64 { return w.amount }

// ParseWithdraw notification into withdraw structure.
func ParseWithdraw(params []stackitem.Item) (event.Event, error) {
	var ev Withdraw

	if ln := len(params); ln != 3 {
		return nil, event.WrongNumberOfParameters(3, ln)
	}

	// parse user
	user, err := client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get withdraw user: %w", err)
	}

	ev.user, err = util.Uint160DecodeBytesBE(user)
	if err != nil {
		return nil, fmt.Errorf("could not convert withdraw user to uint160: %w", err)
	}

	// parse amount
	ev.amount, err = client.IntFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get withdraw amount: %w", err)
	}

	// parse id
	ev.id, err = client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, fmt.Errorf("could not get withdraw id: %w", err)
	}

	return ev, nil
}
