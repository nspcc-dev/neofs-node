package audit

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	auditAPI "github.com/nspcc-dev/neofs-sdk-go/audit"
)

// GetAuditResult returns audit result structure stored in audit contract.
func (c *Client) GetAuditResult(id ResultID) (*auditAPI.Result, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(getResultMethod)
	prm.SetArgs([]byte(id))

	prms, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", getResultMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", getResultMethod, ln)
	}

	value, err := client.BytesFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", getResultMethod, err)
	}

	var auditRes auditAPI.Result
	if err := auditRes.Unmarshal(value); err != nil {
		return nil, fmt.Errorf("could not unmarshal audit result structure: %w", err)
	}

	return &auditRes, nil
}
