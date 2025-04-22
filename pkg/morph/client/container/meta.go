package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// SubmitObjectPut puts object meta information. With awaitTX it blocks until
// TX is accepted in chain or is expired.
//
// Returns any error encountered that caused the saving to interrupt.
func (c *Client) SubmitObjectPut(awaitTX bool, meta []byte, sigs [][]byte) error {
	if len(meta) == 0 || len(sigs) == 0 {
		return errNilArgument
	}

	var prm client.InvokePrm
	prm.SetMethod(submitObjectPutMethod)
	prm.SetArgs(meta, sigs)
	prm.RequireAlphabetSignature()
	if awaitTX {
		prm.Await()
	}

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", submitObjectPutMethod, err)
	}

	return nil
}
