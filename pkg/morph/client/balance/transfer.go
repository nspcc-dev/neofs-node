package balance

import (
	"fmt"
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
	return c.transferX(false, args)
}

// TransferXNotary invokes the call of "transferX" method
// of NeoFS Balance contract via notary contract.
func (c *Client) TransferXNotary(args TransferXArgs) error {
	return c.transferX(true, args)
}

func (c *Client) transferX(notary bool, args TransferXArgs) error {
	f := c.client.Invoke
	if notary {
		f = c.client.NotaryInvoke
	}

	err := f(
		c.transferXMethod,
		args.sender,
		args.recipient,
		args.amount,
		args.details,
	)

	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.transferXMethod, err)
	}
	return nil
}
