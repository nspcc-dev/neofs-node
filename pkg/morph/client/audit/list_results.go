package audit

import (
	"crypto/sha256"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// ListAllAuditResultID returns a list of all audit result IDs inside audit contract.
func (c *Client) ListAllAuditResultID() ([]ResultID, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(listResultsMethod)

	items, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listResultsMethod, err)
	}
	return parseAuditResults(items, listResultsMethod)
}

// ListAuditResultIDByEpoch returns a list of audit result IDs inside audit
// contract for specific epoch number.
func (c *Client) ListAuditResultIDByEpoch(epoch uint64) ([]ResultID, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(listByEpochResultsMethod)
	prm.SetArgs(epoch)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listByEpochResultsMethod, err)
	}
	return parseAuditResults(items, listByEpochResultsMethod)
}

// ListAuditResultIDByCID returns a list of audit result IDs inside audit
// contract for specific epoch number and container ID.
func (c *Client) ListAuditResultIDByCID(epoch uint64, cnr cid.ID) ([]ResultID, error) {
	binCnr := make([]byte, sha256.Size)
	cnr.Encode(binCnr)

	prm := client.TestInvokePrm{}
	prm.SetMethod(listByCIDResultsMethod)
	prm.SetArgs(epoch, binCnr)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listByCIDResultsMethod, err)
	}
	return parseAuditResults(items, listByCIDResultsMethod)
}

// ListAuditResultIDByNode returns a list of audit result IDs inside audit
// contract for specific epoch number, container ID and inner ring public key.
func (c *Client) ListAuditResultIDByNode(epoch uint64, cnr cid.ID, nodeKey []byte) ([]ResultID, error) {
	binCnr := make([]byte, sha256.Size)
	cnr.Encode(binCnr)

	prm := client.TestInvokePrm{}
	prm.SetMethod(listByNodeResultsMethod)
	prm.SetArgs(epoch, binCnr, nodeKey)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listByNodeResultsMethod, err)
	}
	return parseAuditResults(items, listByNodeResultsMethod)
}

func parseAuditResults(items []stackitem.Item, method string) ([]ResultID, error) {
	if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", method, ln)
	}

	items, err := client.ArrayFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", method, err)
	}

	res := make([]ResultID, 0, len(items))
	for i := range items {
		rawRes, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", method, err)
		}

		res = append(res, rawRes)
	}

	return res, nil
}
