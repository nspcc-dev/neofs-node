package balance

import (
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// TransferPrm groups parameters of TransferX method.
type TransferPrm struct {
	Amount *big.Int

	From, To user.ID

	Details []byte

	client.InvokePrmOptional
}

// TransferX transfers p.Amount of GASe-12 from p.From to p.To
// with details p.Details through direct smart contract call.
func (c *Client) TransferX(p TransferPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(transferXMethod)
	prm.SetArgs(p.From.ScriptHash(), p.To.ScriptHash(), p.Amount, p.Details)
	prm.InvokePrmOptional = p.InvokePrmOptional

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", transferXMethod, err)
	}
	return nil
}
