package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// Quota represents set quota limits.
type Quota struct {
	SoftLimit uint64
	HardLimit uint64
}

// FromStackItem implements [stackitem.Convertible].
func (q *Quota) FromStackItem(stack stackitem.Item) error {
	quotaStruct, err := client.ArrayFromStackItem(stack)
	if err != nil {
		return fmt.Errorf("could not read stack as array: %w", err)
	}
	if l := len(quotaStruct); l != 2 {
		return fmt.Errorf("summary struct has unexpected number of elements: %d (%d expected)", l, 2)
	}
	softLim, err := client.IntFromStackItem(quotaStruct[0])
	if err != nil {
		return fmt.Errorf("could not read soft limit: %w", err)
	}
	if softLim < 0 {
		return fmt.Errorf("soft limit is negative: %d", softLim)
	}
	hardLim, err := client.IntFromStackItem(quotaStruct[1])
	if err != nil {
		return fmt.Errorf("could not read hard limit: %w", err)
	}
	if hardLim < 0 {
		return fmt.Errorf("hard limit is negative: %d", hardLim)
	}

	q.SoftLimit = uint64(softLim)
	q.HardLimit = uint64(hardLim)

	return nil
}

// GetContainerQuota returns set container size quotas according to contract
// state. If no limit set, zero value is retuned.
func (c *Client) GetContainerQuota(cID cid.ID) (Quota, error) {
	return c.getQuota(fschaincontracts.GetContainerQuotaMethod, cID[:])
}

// GetUserQuota returns set user size quotas according to contract state.
// If no limit set, zero value is retuned.
func (c *Client) GetUserQuota(user user.ID) (Quota, error) {
	return c.getQuota(fschaincontracts.GetUserQuotaMethod, user[:])
}

func (c *Client) getQuota(method string, arg []byte) (Quota, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(method)
	prm.SetArgs(arg)

	var q Quota
	res, err := c.client.TestInvoke(prm)
	if err != nil {
		return q, fmt.Errorf("could not invoke method (%s): %w", method, err)
	}
	if ln := len(res); ln != 1 {
		return q, fmt.Errorf("unexpected stack item count (%s): %d", method, ln)
	}
	err = q.FromStackItem(res[0])
	if err != nil {
		return q, fmt.Errorf("reading quota structure from stack: %w", err)
	}

	return q, nil
}
