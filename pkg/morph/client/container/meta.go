package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// SubmitObjectPut puts object meta information.
//
// Returns any error encountered that caused the saving to interrupt.
func (c *Client) SubmitObjectPut(meta []byte, sigs [][][]byte) error {
	if len(meta) == 0 || len(sigs) == 0 {
		return errNilArgument
	}

	var prm client.InvokePrm
	prm.SetMethod(submitObjectPutMethod)
	prm.SetArgs(meta, sigs)
	prm.PayByProxy()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", submitObjectPutMethod, err)
	}

	return nil
}
