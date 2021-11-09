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

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.transferXMethod, err)
	}

	return nil
}

// Mint invokes `mint` method of the balance contract.
func (c *Client) Mint(to []byte, amount int64, id []byte) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.mintMethod)
	prm.SetArgs(to, amount, id)

	return c.client.Invoke(prm)
}

// Burn invokes `burn` method of the balance contract.
func (c *Client) Burn(to []byte, amount int64, id []byte) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.burnMethod)
	prm.SetArgs(to, amount, id)

	return c.client.Invoke(prm)
}

// Lock invokes `lock` method of the balance contract.
func (c *Client) Lock(id, user, lock []byte, amount, dueEpoch int64) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.lockMethod)
	prm.SetArgs(id, user, lock, amount, dueEpoch)

	return c.client.Invoke(prm)
}
