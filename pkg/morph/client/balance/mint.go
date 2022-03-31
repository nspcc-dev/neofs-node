package balance

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

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
func (c *Client) Mint(p MintPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(mintMethod)
	prm.SetArgs(p.to, p.amount, p.id)
	prm.InvokePrmOptional = p.InvokePrmOptional

	return c.client.Invoke(prm)
}
