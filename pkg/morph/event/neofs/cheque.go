package neofs

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

// Cheque structure of neofs.Cheque notification from mainnet chain.
type Cheque struct {
	id     []byte
	amount int64 // Fixed8
	user   util.Uint160
	lock   util.Uint160
}

// MorphEvent implements Neo:Morph Event interface.
func (Cheque) MorphEvent() {}

// ID is a withdraw transaction hash.
func (c Cheque) ID() []byte { return c.id }

// User returns withdraw receiver script hash from main net.
func (c Cheque) User() util.Uint160 { return c.user }

// Amount of the sent assets.
func (c Cheque) Amount() int64 { return c.amount }

// LockAccount return script hash for balance contract wallet.
func (c Cheque) LockAccount() util.Uint160 { return c.lock }

// ParseCheque from notification into cheque structure.
func ParseCheque(params []stackitem.Item) (event.Event, error) {
	var (
		ev  Cheque
		err error
	)

	if ln := len(params); ln != 4 {
		return nil, event.WrongNumberOfParameters(4, ln)
	}

	// parse id
	ev.id, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get cheque id")
	}

	// parse user
	user, err := client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get cheque user")
	}

	ev.user, err = util.Uint160DecodeBytesBE(user)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert cheque user to uint160")
	}

	// parse amount
	ev.amount, err = client.IntFromStackItem(params[2])
	if err != nil {
		return nil, errors.Wrap(err, "could not get cheque amount")
	}

	// parse lock account
	lock, err := client.BytesFromStackItem(params[3])
	if err != nil {
		return nil, errors.Wrap(err, "could not get cheque lock account")
	}

	ev.lock, err = util.Uint160DecodeBytesBE(lock)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert cheque lock account to uint160")
	}

	return ev, nil
}
