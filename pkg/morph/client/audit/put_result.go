package audit

import (
	"fmt"
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
	err := c.client.Invoke(
		c.putResultMethod,
		args.rawResult,
	)

	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.putResultMethod, err)
	}
	return nil
}
