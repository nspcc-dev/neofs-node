package balance

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

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

// SetID sets ID.
func (b *BurnPrm) SetID(id []byte) {
	b.id = id
}

// Burn destroys funds from the account.
func (c *Client) Burn(p BurnPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(burnMethod)
	prm.SetArgs(p.to, p.amount, p.id)
	prm.InvokePrmOptional = p.InvokePrmOptional

	return c.client.Invoke(prm)
}
