package audit

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditAPI "github.com/nspcc-dev/neofs-sdk-go/audit"
)

// ResultID is an identity of audit result inside audit contract.
type ResultID []byte

// PutPrm groups parameters of PutAuditResult operation.
type PutPrm struct {
	result *auditAPI.Result

	client.InvokePrmOptional
}

// SetResult sets audit result.
func (p *PutPrm) SetResult(result *auditAPI.Result) {
	p.result = result
}

// PutAuditResult saves passed audit result structure in NeoFS system
// through Audit contract call.
//
// Returns encountered error that caused the saving to interrupt.
func (c *Client) PutAuditResult(p PutPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(putResultMethod)
	prm.SetArgs(p.result.Marshal())
	prm.InvokePrmOptional = p.InvokePrmOptional

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", putResultMethod, err)
	}
	return nil
}
