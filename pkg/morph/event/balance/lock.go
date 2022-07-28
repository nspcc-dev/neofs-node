package balance

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
)

// Lock structure of balance.Lock notification from morph chain.
type Lock struct {
	id     []byte
	user   util.Uint160
	lock   util.Uint160
	amount int64 // Fixed16
	until  int64

	// txHash is used in notary environmental
	// for calculating unique but same for
	// all notification receivers values.
	txHash util.Uint256
}

// MorphEvent implements Neo:Morph Event interface.
func (Lock) MorphEvent() {}

// ID is a withdraw transaction hash.
func (l Lock) ID() []byte { return l.id }

// User returns withdraw receiver script hash from main net.
func (l Lock) User() util.Uint160 { return l.user }

// LockAccount return script hash for balance contract wallet.
func (l Lock) LockAccount() util.Uint160 { return l.lock }

// Amount of the locked assets.
func (l Lock) Amount() int64 { return l.amount }

// Until is an epoch before locked account exists.
func (l Lock) Until() int64 { return l.until }

// TxHash returns hash of the TX with lock
// notification.
func (l Lock) TxHash() util.Uint256 { return l.txHash }

// ParseLock from notification into lock structure.
func ParseLock(e *state.ContainedNotificationEvent) (event.Event, error) {
	var (
		ev  Lock
		err error
	)

	params, err := event.ParseStackArray(e)
	if err != nil {
		return nil, fmt.Errorf("could not parse stack items from notify event: %w", err)
	}

	if ln := len(params); ln != 5 {
		return nil, event.WrongNumberOfParameters(5, ln)
	}

	// parse id
	ev.id, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, fmt.Errorf("could not get lock id: %w", err)
	}

	// parse user
	user, err := client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, fmt.Errorf("could not get lock user value: %w", err)
	}

	ev.user, err = util.Uint160DecodeBytesBE(user)
	if err != nil {
		return nil, fmt.Errorf("could not convert lock user value to uint160: %w", err)
	}

	// parse lock account
	lock, err := client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, fmt.Errorf("could not get lock account value: %w", err)
	}

	ev.lock, err = util.Uint160DecodeBytesBE(lock)
	if err != nil {
		return nil, fmt.Errorf("could not convert lock account value to uint160: %w", err)
	}

	// parse amount
	ev.amount, err = client.IntFromStackItem(params[3])
	if err != nil {
		return nil, fmt.Errorf("could not get lock amount: %w", err)
	}

	// parse until deadline
	ev.until, err = client.IntFromStackItem(params[4])
	if err != nil {
		return nil, fmt.Errorf("could not get lock deadline: %w", err)
	}

	ev.txHash = e.Container

	return ev, nil
}
