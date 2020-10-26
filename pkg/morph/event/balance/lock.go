package balance

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/pkg/errors"
)

// Lock structure of balance.Lock notification from morph chain.
type Lock struct {
	id     []byte
	user   util.Uint160
	lock   util.Uint160
	amount int64 // Fixed16
	until  int64
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

// Until is a epoch before locked account exists.
func (l Lock) Until() int64 { return l.until }

// ParseLock from notification into lock structure.
func ParseLock(params []stackitem.Item) (event.Event, error) {
	var (
		ev  Lock
		err error
	)

	if ln := len(params); ln != 5 {
		return nil, event.WrongNumberOfParameters(5, ln)
	}

	// parse id
	ev.id, err = client.BytesFromStackItem(params[0])
	if err != nil {
		return nil, errors.Wrap(err, "could not get lock id")
	}

	// parse user
	user, err := client.BytesFromStackItem(params[1])
	if err != nil {
		return nil, errors.Wrap(err, "could not get lock user value")
	}

	ev.user, err = util.Uint160DecodeBytesBE(user)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert lock user value to uint160")
	}

	// parse lock account
	lock, err := client.BytesFromStackItem(params[2])
	if err != nil {
		return nil, errors.Wrap(err, "could not get lock account value")
	}

	ev.lock, err = util.Uint160DecodeBytesBE(lock)
	if err != nil {
		return nil, errors.Wrap(err, "could not convert lock account value to uint160")
	}

	// parse amount
	ev.amount, err = client.IntFromStackItem(params[3])
	if err != nil {
		return nil, errors.Wrap(err, "could not get lock amount")
	}

	// parse until deadline
	ev.until, err = client.IntFromStackItem(params[4])
	if err != nil {
		return nil, errors.Wrap(err, "could not get lock deadline")
	}

	return ev, nil
}
