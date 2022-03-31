package balance

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// LockPrm groups parameters of Lock operation.
type LockPrm struct {
	id       []byte
	user     util.Uint160
	lock     util.Uint160
	amount   int64
	dueEpoch int64

	client.InvokePrmOptional
}

// SetID sets ID.
func (l *LockPrm) SetID(id []byte) {
	l.id = id
}

// SetUser set user.
func (l *LockPrm) SetUser(user util.Uint160) {
	l.user = user
}

// SetLock sets lock.
func (l *LockPrm) SetLock(lock util.Uint160) {
	l.lock = lock
}

// SetAmount sets amount.
func (l *LockPrm) SetAmount(amount int64) {
	l.amount = amount
}

// SetDueEpoch sets end of the lock.
func (l *LockPrm) SetDueEpoch(dueEpoch int64) {
	l.dueEpoch = dueEpoch
}

// Lock locks fund on the user account.
func (c *Client) Lock(p LockPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(lockMethod)
	prm.SetArgs(p.id, p.user, p.lock, p.amount, p.dueEpoch)
	prm.InvokePrmOptional = p.InvokePrmOptional

	return c.client.Invoke(prm)
}
