package container

import (
	"fmt"

	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// AnnounceLoadPrm groups parameters of AnnounceLoad operation.
type AnnounceLoadPrm struct {
	a   container.UsedSpaceAnnouncement
	key []byte

	client.InvokePrmOptional
}

// SetAnnouncement sets announcement.
func (a2 *AnnounceLoadPrm) SetAnnouncement(a container.UsedSpaceAnnouncement) {
	a2.a = a
}

// SetReporter sets public key of the reporter.
func (a2 *AnnounceLoadPrm) SetReporter(key []byte) {
	a2.key = key
}

// AnnounceLoad saves container size estimation calculated by storage node
// with key in NeoFS system through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (c *Client) AnnounceLoad(p AnnounceLoadPrm) error {
	v2 := p.a.ContainerID().ToV2()
	if v2 == nil {
		return errUnsupported // use other major version if there any
	}

	prm := client.InvokePrm{}
	prm.SetMethod(putSizeMethod)
	prm.SetArgs(p.a.Epoch(), v2.GetValue(), p.a.UsedSpace(), p.key)
	prm.InvokePrmOptional = p.InvokePrmOptional

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", putSizeMethod, err)
	}
	return nil
}

// EstimationID is an identity of container load estimation inside Container contract.
type EstimationID []byte

// ListLoadEstimationsByEpoch returns a list of container load estimations for to the specified epoch.
// The list is composed through Container contract call.
func (c *Client) ListLoadEstimationsByEpoch(epoch uint64) ([]EstimationID, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(listSizesMethod)
	invokePrm.SetArgs(epoch)

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listSizesMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", listSizesMethod, ln)
	}

	prms, err = client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", listSizesMethod, err)
	}

	res := make([]EstimationID, 0, len(prms))
	for i := range prms {
		id, err := client.BytesFromStackItem(prms[i])
		if err != nil {
			return nil, fmt.Errorf("could not get ID byte array from stack item (%s): %w", listSizesMethod, err)
		}

		res = append(res, id)
	}

	return res, nil
}

// Estimation is a structure of single container load estimation
// reported by storage node.
type Estimation struct {
	Size uint64

	Reporter []byte
}

// Estimations is a structure of grouped container load estimation inside Container contract.
type Estimations struct {
	ContainerID *cid.ID

	Values []Estimation
}

// GetUsedSpaceEstimations returns a list of container load estimations by ID.
// The list is composed through Container contract call.
func (c *Client) GetUsedSpaceEstimations(id EstimationID) (*Estimations, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(getSizeMethod)
	prm.SetArgs([]byte(id))

	prms, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", getSizeMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", getSizeMethod, ln)
	}

	prms, err = client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack items of estimation fields from stack item (%s): %w", getSizeMethod, err)
	} else if ln := len(prms); ln != 2 {
		return nil, fmt.Errorf("unexpected stack item count of estimations fields (%s)", getSizeMethod)
	}

	rawCID, err := client.BytesFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get container ID byte array from stack item (%s): %w", getSizeMethod, err)
	}

	prms, err = client.ArrayFromStackItem(prms[1])
	if err != nil {
		return nil, fmt.Errorf("could not get estimation list array from stack item (%s): %w", getSizeMethod, err)
	}

	v2 := new(v2refs.ContainerID)
	v2.SetValue(rawCID)
	res := &Estimations{
		ContainerID: cid.NewFromV2(v2),
		Values:      make([]Estimation, 0, len(prms)),
	}

	for i := range prms {
		arr, err := client.ArrayFromStackItem(prms[i])
		if err != nil {
			return nil, fmt.Errorf("could not get estimation struct from stack item (%s): %w", getSizeMethod, err)
		} else if ln := len(arr); ln != 2 {
			return nil, fmt.Errorf("unexpected stack item count of estimation fields (%s)", getSizeMethod)
		}

		reporter, err := client.BytesFromStackItem(arr[0])
		if err != nil {
			return nil, fmt.Errorf("could not get reporter byte array from stack item (%s): %w", getSizeMethod, err)
		}

		sz, err := client.IntFromStackItem(arr[1])
		if err != nil {
			return nil, fmt.Errorf("could not get estimation size from stack item (%s): %w", getSizeMethod, err)
		}

		res.Values = append(res.Values, Estimation{
			Reporter: reporter,
			Size:     uint64(sz),
		})
	}

	return res, nil
}
