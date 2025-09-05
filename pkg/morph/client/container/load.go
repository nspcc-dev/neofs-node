package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// Report is a structure of single container load reported by a storage node.
type Report struct {
	StorageSize   int64
	ObjectsNumber int64
	ReportsNumber int64
	Reporter      []byte
}

// FromStackItem implements stackitem.Convertible.
func (r *Report) FromStackItem(item stackitem.Item) error {
	v, err := client.ArrayFromStackItem(item)
	if err != nil {
		return fmt.Errorf("incorrect array from stack item: %w", err)
	}

	if len(v) != 4 { // 4 field resulting struct
		return fmt.Errorf("incorrect report struct size: %d", len(v))
	}

	r.Reporter, err = client.BytesFromStackItem(v[0])
	if err != nil {
		return fmt.Errorf("incorrect reporter key: %w", err)
	}
	r.StorageSize, err = client.IntFromStackItem(v[1])
	if err != nil {
		return fmt.Errorf("incorrect container size: %w", err)
	}
	r.ObjectsNumber, err = client.IntFromStackItem(v[2])
	if err != nil {
		return fmt.Errorf("incorrect objects number: %w", err)
	}
	r.ReportsNumber, err = client.IntFromStackItem(v[3])
	if err != nil {
		return fmt.Errorf("incorrect number of reports: %w", err)
	}

	return nil
}

// PutReport saves container state reported by storage node with key in NeoFS
// system through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (c *Client) PutReport(cID cid.ID, storageSize, objsNumber uint64, key []byte) error {
	prm := client.InvokePrm{}
	prm.SetMethod(fschaincontracts.PutContainerReportMethod)
	prm.SetArgs(cID[:], storageSize, objsNumber, key)

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.PutContainerReportMethod, err)
	}
	return nil
}

// NodeReports returns a list of container load reports for to the
// specified epoch.
// The list is composed through Container contract call.
func (c *Client) NodeReports(epoch uint64, cID cid.ID) ([]Report, error) {
	rr, err := c.client.TestInvokeIterator(fschaincontracts.IterateContainerReportsMethod, iteratorPrefetchNumber, epoch, cID[:])
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", fschaincontracts.IterateContainerReportsMethod, err)
	}

	res := make([]Report, len(rr))
	for i := range rr {
		err = res[i].FromStackItem(rr[i])
		if err != nil {
			return nil, fmt.Errorf("reading report from stack: %w", err)
		}
	}

	return res, nil
}
