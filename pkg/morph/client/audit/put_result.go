package audit

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// PutAuditResultArgs groups the arguments
// of "put audit result" invocation call.
type PutAuditResultArgs struct {
	rawResult []byte // audit result in NeoFS API-compatible binary representation
}

// SetRawResult sets audit result structure
// in NeoFS API-compatible binary representation.
func (g *PutAuditResultArgs) SetRawResult(v []byte) {
	g.rawResult = v
}

// PutAuditResult invokes the call of "put audit result" method
// of NeoFS Audit contract.
func (c *Client) PutAuditResult(args PutAuditResultArgs) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.putResultMethod)
	prm.SetArgs(args.rawResult)

	err := c.client.Invoke(prm)

	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.putResultMethod, err)
	}
	return nil
}
