package balance

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// TransferXArgs groups the arguments
// of "transferX" invocation call.
type TransferXArgs struct {
	amount int64 // amount in GASe-12

	sender []byte // sender's wallet script hash

	recipient []byte // recipient's wallet script hash

	details []byte // transfer details

	client.InvokePrmOptional
}

// SetAmount sets amount of funds to transfer
// in GASe-12.
func (t *TransferXArgs) SetAmount(v int64) {
	t.amount = v
}

// SetSender sets wallet script hash
// of the sender of funds in a binary format.
func (t *TransferXArgs) SetSender(v []byte) {
	t.sender = v
}

// SetRecipient sets wallet script hash
// of the recipient of funds in a binary format.
func (t *TransferXArgs) SetRecipient(v []byte) {
	t.recipient = v
}

// SetDetails sets details of the money transaction
// in a binary format.
func (t *TransferXArgs) SetDetails(v []byte) {
	t.details = v
}

// TransferX directly invokes the call of "transferX" method
// of NeoFS Balance contract.
func (c *Client) TransferX(args TransferXArgs) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.transferXMethod)
	prm.SetArgs(args.sender, args.recipient, args.amount, args.details)
	prm.InvokePrmOptional = args.InvokePrmOptional

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.transferXMethod, err)
	}

	return nil
}

// MintPrm groups parameters of Mint operation.
type MintPrm struct {
	to     []byte
	amount int64
	id     []byte

	client.InvokePrmOptional
}

// SetTo sets receiver of the transfer.
func (m *MintPrm) SetTo(to []byte) {
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

// Mint invokes `mint` method of the balance contract.
func (c *Client) Mint(args MintPrm) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.mintMethod)
	prm.SetArgs(args.to, args.amount, args.id)
	prm.InvokePrmOptional = args.InvokePrmOptional

	return c.client.Invoke(prm)
}

// BurnPrm groups parameters of Burn operation.
type BurnPrm struct {
	to     []byte
	amount int64
	id     []byte

	client.InvokePrmOptional
}

// SetTo sets receiver.
func (b *BurnPrm) SetTo(to []byte) {
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

// Burn invokes `burn` method of the balance contract.
func (c *Client) Burn(args BurnPrm) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.burnMethod)
	prm.SetArgs(args.to, args.amount, args.id)
	prm.InvokePrmOptional = args.InvokePrmOptional

	return c.client.Invoke(prm)
}

// LockPrm groups parameters of Lock operation.
type LockPrm struct {
	id       []byte
	user     []byte
	lock     []byte
	amount   int64
	dueEpoch int64

	client.InvokePrmOptional
}

// SetID sets ID.
func (l *LockPrm) SetID(id []byte) {
	l.id = id
}

// SetUser set user.
func (l *LockPrm) SetUser(user []byte) {
	l.user = user
}

// SetLock sets lock.
func (l *LockPrm) SetLock(lock []byte) {
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

// Lock invokes `lock` method of the balance contract.
func (c *Client) Lock(args LockPrm) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.lockMethod)
	prm.SetArgs(args.id, args.user, args.lock, args.amount, args.dueEpoch)
	prm.InvokePrmOptional = args.InvokePrmOptional

	return c.client.Invoke(prm)
}
