package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// Report is a structure of single container load reported by a storage node.
type Report struct {
	StorageSize     int64
	ObjectsNumber   int64
	ReportsNumber   int64
	Reporter        []byte
	LastUpdateEpoch int64
}

// FromStackItem implements stackitem.Convertible.
func (r *Report) FromStackItem(item stackitem.Item) error {
	v, err := client.ArrayFromStackItem(item)
	if err != nil {
		return fmt.Errorf("incorrect array from stack item: %w", err)
	}

	if len(v) != 5 { // 5 fields resulting struct
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
	r.LastUpdateEpoch, err = client.IntFromStackItem(v[4])
	if err != nil {
		return fmt.Errorf("incorrect last update's epoch: %w", err)
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

// NodeReports returns a list of container load reports.
// The list is composed through Container contract call.
func (c *Client) NodeReports(cID cid.ID) ([]Report, error) {
	rr, err := c.client.TestInvokeIterator(fschaincontracts.IterateContainerReportsMethod, iteratorPrefetchNumber, cID[:])
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

// Summary represents summary container report.
type Summary struct {
	Size            uint64
	NumberOfObjects uint64
}

// FromStackItem implements [stackitem.Convertible].
func (s *Summary) FromStackItem(item stackitem.Item) error {
	sumStruct, err := client.ArrayFromStackItem(item)
	if err != nil {
		return fmt.Errorf("could not read stack as array: %w", err)
	}
	if l := len(sumStruct); l != 2 {
		return fmt.Errorf("summary struct has unexpected number of elements: %d (%d expected)", l, 2)
	}
	size, err := client.IntFromStackItem(sumStruct[0])
	if err != nil {
		return fmt.Errorf("could not read container size: %w", err)
	}
	if size < 0 {
		return fmt.Errorf("container size is negative: %d", size)
	}
	numberOfObjects, err := client.IntFromStackItem(sumStruct[1])
	if err != nil {
		return fmt.Errorf("could not read number of objects: %w", err)
	}
	if numberOfObjects < 0 {
		return fmt.Errorf("number of objects is negative: %d", numberOfObjects)
	}

	s.Size = uint64(size)
	s.NumberOfObjects = uint64(numberOfObjects)

	return nil
}

// GetReportsSummary returns summary report based on preceding [PutReport]
// calls made by storage nodes.
func (c *Client) GetReportsSummary(cID cid.ID) (Summary, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(fschaincontracts.GetReportsSummaryMethod)
	prm.SetArgs(cID[:])

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		return Summary{}, fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.GetReportsSummaryMethod, err)
	}
	if ln := len(res); ln != 1 {
		return Summary{}, fmt.Errorf("unexpected stack item count (%s): %d", fschaincontracts.GetReportsSummaryMethod, ln)
	}
	var s Summary
	err = s.FromStackItem(res[0])
	if err != nil {
		return Summary{}, fmt.Errorf("reading reports summary from stack: %w", err)
	}

	return s, nil
}

// GetTakenSpaceByUser returns a sum of all taken space in every container that
// belongs to user.
func (c *Client) GetTakenSpaceByUser(user user.ID) (uint64, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(fschaincontracts.GetTakenSpaceByUserMethod)
	prm.SetArgs(user[:])

	res, err := c.client.TestInvoke(prm)
	if err != nil {
		return 0, fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.GetTakenSpaceByUserMethod, err)
	}
	if ln := len(res); ln != 1 {
		return 0, fmt.Errorf("unexpected stack item count (%s): %d", fschaincontracts.GetTakenSpaceByUserMethod, ln)
	}
	size, err := client.IntFromStackItem(res[0])
	if err != nil {
		return 0, fmt.Errorf("reading integer result from stack: %w", err)
	}
	if size < 0 {
		return 0, fmt.Errorf("taken space is negative: %d", size)
	}

	return uint64(size), nil
}
