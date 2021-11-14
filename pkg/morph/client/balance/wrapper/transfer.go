package wrapper

import (
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
)

// TransferPrm groups parameters of TransferX method.
type TransferPrm struct {
	Amount int64

	From, To *owner.ID

	Details []byte

	client.InvokePrmOptional
}

// TransferX transfers p.Amount of GASe-12 from p.From to p.To
// with details p.Details through direct smart contract call.
//
// If TryNotary is provided, calls notary contract.
func (w *Wrapper) TransferX(p TransferPrm) error {
	from, err := address.StringToUint160(p.From.String())
	if err != nil {
		return err
	}
	to, err := address.StringToUint160(p.From.String())
	if err != nil {
		return err
	}

	// prepare invocation arguments
	args := balance.TransferXArgs{}
	args.SetSender(from.BytesBE())
	args.SetRecipient(to.BytesBE())
	args.SetAmount(p.Amount)
	args.SetDetails(p.Details)
	args.InvokePrmOptional = p.InvokePrmOptional

	return w.client.TransferX(args)
}

// MintPrm groups parameters of Mint operation.
type MintPrm struct {
	to     util.Uint160
	amount int64
	id     []byte

	client.InvokePrmOptional
}

// SetTo sets receiver of the transfer.
func (m *MintPrm) SetTo(to util.Uint160) {
	m.to = to
}

// SetAmount sets amount of the transfer.
func (m *MintPrm) SetAmount(amount int64) {
	m.amount = amount
}

// SetID sets ID.
func (m *MintPrm) SetID(id []byte) {
	m.id = id
}

// Mint sends funds to the account.
func (w *Wrapper) Mint(prm MintPrm) error {
	args := balance.MintPrm{}

	args.SetTo(prm.to.BytesBE())
	args.SetAmount(prm.amount)
	args.SetID(prm.id)
	args.InvokePrmOptional = prm.InvokePrmOptional

	return w.client.Mint(args)
}

// BurnPrm groups parameters of Burn operation.
type BurnPrm struct {
	to     util.Uint160
	amount int64
	id     []byte

	client.InvokePrmOptional
}

// SetTo sets receiver.
func (b *BurnPrm) SetTo(to util.Uint160) {
	b.to = to
}

// SetAmount sets amount.
func (b *BurnPrm) SetAmount(amount int64) {
	b.amount = amount
}

// SetID sets ID
func (b *BurnPrm) SetID(id []byte) {
	b.id = id
}

// Burn destroys funds from the account.
func (w *Wrapper) Burn(prm BurnPrm) error {
	args := balance.BurnPrm{}

	args.SetTo(prm.to.BytesBE())
	args.SetAmount(prm.amount)
	args.SetID(prm.id)
	args.InvokePrmOptional = prm.InvokePrmOptional

	return w.client.Burn(args)
}

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
func (w *Wrapper) Lock(prm LockPrm) error {
	args := balance.LockPrm{}

	args.SetID(prm.id)
	args.SetUser(prm.user.BytesBE())
	args.SetLock(prm.lock.BytesBE())
	args.SetAmount(prm.amount)
	args.SetDueEpoch(prm.dueEpoch)
	args.InvokePrmOptional = prm.InvokePrmOptional

	return w.client.Lock(args)
}
