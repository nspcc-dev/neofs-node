package container

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// EstimationV2 is a structure of grouped container load estimation inside Container contract.
type EstimationV2 struct {
	CID             cid.ID
	StorageSize     int64
	NumberOfObjects int64
}

// ToStackItem implements [stackitem.Convertible].
func (e *EstimationV2) ToStackItem() (stackitem.Item, error) {
	panic("not for now")
}

// FromStackItem implements [stackitem.Convertible].
func (e *EstimationV2) FromStackItem(item stackitem.Item) error {
	v, err := client.ArrayFromStackItem(item)
	if err != nil {
		return fmt.Errorf("incorrect report value from iterator: %w", err)
	}

	if len(v) != 2 { // 2 field resulting struct
		return fmt.Errorf("incorrect report struct size: %d", len(v))
	}

	e.StorageSize, err = client.IntFromStackItem(v[0])
	if err != nil {
		return fmt.Errorf("incorrect reporter key %w", err)
	}
	e.NumberOfObjects, err = client.IntFromStackItem(v[1])
	if err != nil {
		return fmt.Errorf("incorrect container size: %w", err)
	}

	return nil
}

// Report is a structure of single container load reported by a storage node.
type Report struct {
	StorageSize   int64
	ObjectsNumber int64
	ReportsNumber int64
	Reporter      []byte
}

// ToStackItem implements stackitem.Convertible.
func (r *Report) ToStackItem() (stackitem.Item, error) {
	panic("not for now")
}

// FromStackItem implements stackitem.Convertible.
func (r *Report) FromStackItem(item stackitem.Item) error {
	v, err := client.ArrayFromStackItem(item)
	if err != nil {
		return fmt.Errorf("incorrect report value from iterator: %w", err)
	}

	if len(v) != 4 { // 4 field resulting struct
		return fmt.Errorf("incorrect report struct size: %d", len(v))
	}

	r.Reporter, err = client.BytesFromStackItem(v[0])
	if err != nil {
		return fmt.Errorf("incorrect reporter key %w", err)
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
	prm.SetMethod(putReportMethod)
	prm.SetArgs(cID[:], storageSize, objsNumber, key)

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", putReportMethod, err)
	}
	return nil
}

// EstimationsForEpoch returns list of all container size estimations for epoch.
func (c *Client) EstimationsForEpoch(epoch uint64) ([]EstimationV2, error) {
	// neo-go's stack elements default limit
	// is 2048, make it less a little
	const prefetchNumber = 2000

	kvs, err := c.client.TestInvokeIterator(iterateReports, prefetchNumber, epoch)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", iterateAllEstimations, err)
	}

	res := make([]EstimationV2, 0, len(kvs))
	for i := range kvs {
		kv, err := client.ArrayFromStackItem(kvs[i])
		if err != nil {
			return nil, fmt.Errorf("could not unwrap iterator key-value pair: %w", err)
		}
		if len(kv) != 2 { // key-value array
			return nil, fmt.Errorf("incorrect contract key-value struct size: %d", len(kv))
		}

		var e EstimationV2
		e.CID, err = cidFromStorageKey(kv[0])
		if err != nil {
			return nil, fmt.Errorf("storage key handling: %w", err)
		}
		err = e.FromStackItem(kv[1])
		if err != nil {
			return nil, fmt.Errorf("storage value handling: %w", err)
		}

		res = append(res, e)
	}

	return res, nil
}

// NodeReports returns a list of container load reports for to the
// specified epoch.
// The list is composed through Container contract call.
func (c *Client) NodeReports(epoch uint64, cID cid.ID) ([]Report, error) {
	// neo-go's stack elements default limit
	// is 2048, make it less a little
	const prefetchNumber = 2000

	rr, err := c.client.TestInvokeIterator(iterateReports, prefetchNumber, epoch, cID[:])
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", iterateReports, err)
	}

	res := make([]Report, 0, len(rr))
	for i := range rr {
		var r Report
		err = r.FromStackItem(rr[i])
		if err != nil {
			return nil, fmt.Errorf("reading report from stack: %w", err)
		}
		res = append(res, r)
	}

	return res, nil
}
