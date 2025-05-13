package balance

import (
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// TransferX wraps smart contract call and allows to transfer the given amount
// of picoGAS from "from" to "to" account with given details.
func (c *Client) TransferX(from user.ID, to user.ID, amount *big.Int, details []byte) error {
	prm := client.InvokePrm{}
	prm.SetMethod(transferXMethod)
	prm.SetArgs(from.ScriptHash(), to.ScriptHash(), amount, details)

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", transferXMethod, err)
	}
	return nil
}
