package container

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

// PutSizeArgs groups the arguments
// of "put container size" invocation call.
type PutSizeArgs struct {
	epoch int64

	size int64

	cid []byte

	reporterKey []byte
}

// SetEpoch sets the number of the epoch when
// size was estimated.
func (p *PutSizeArgs) SetEpoch(v uint64) {
	p.epoch = int64(v)
}

// SetSize sets estimation of the container size.
func (p *PutSizeArgs) SetSize(v uint64) {
	p.size = int64(v)
}

// SetContainerID sets identifier of the container
// being evaluated.
func (p *PutSizeArgs) SetContainerID(v []byte) {
	p.cid = v
}

// SetReporterKey ыуеы public key of the storage node
// that collected size estimation.
func (p *PutSizeArgs) SetReporterKey(v []byte) {
	p.reporterKey = v
}

// Put invokes the call of put container method
// of NeoFS Container contract.
func (c *Client) PutSize(args PutSizeArgs) error {
	return errors.Wrapf(c.client.Invoke(
		c.putSizeMethod,
		args.epoch,
		args.cid,
		args.size,
		args.reporterKey,
	), "could not invoke method (%s)", c.putSizeMethod)
}

// ListSizesArgs groups the arguments
// of "list container sizes" test invoke call..
type ListSizesArgs struct {
	epoch int64
}

// SetEpoch sets the number of the epoch to select
// the estimations.
func (p *ListSizesArgs) SetEpoch(v uint64) {
	p.epoch = int64(v)
}

// ListSizesValues groups the stack items
// returned by "list container sizes" test invoke.
type ListSizesValues struct {
	ids [][]byte
}

// Announcements returns list of identifiers of the
// container load estimations.
func (v *ListSizesValues) IDList() [][]byte {
	return v.ids
}

// List performs the test invoke of "list container sizes"
// method of NeoFS Container contract.
func (c *Client) ListSizes(args ListSizesArgs) (*ListSizesValues, error) {
	prms, err := c.client.TestInvoke(
		c.listSizesMethod,
		args.epoch,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.listSizesMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.listSizesMethod, ln)
	}

	prms, err = client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack item array from stack item (%s)", c.listSizesMethod)
	}

	res := &ListSizesValues{
		ids: make([][]byte, 0, len(prms)),
	}

	for i := range prms {
		id, err := client.BytesFromStackItem(prms[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not get ID byte array from stack item (%s)", c.listSizesMethod)
		}

		res.ids = append(res.ids, id)
	}

	return res, nil
}

// GetSizeArgs groups the arguments
// of "get container size" test invoke call..
type GetSizeArgs struct {
	id []byte
}

// SetID sets identifier of the container estimation.
func (p *GetSizeArgs) SetID(v []byte) {
	p.id = v
}

type Estimation struct {
	Size int64

	Reporter []byte
}

type Estimations struct {
	ContainerID []byte

	Estimations []Estimation
}

// GetSizeValues groups the stack items
// returned by "get container size" test invoke.
type GetSizeValues struct {
	est Estimations
}

// Estimations returns list of the container load estimations.
func (v *GetSizeValues) Estimations() Estimations {
	return v.est
}

// GetContainerSize performs the test invoke of "get container size"
// method of NeoFS Container contract.
func (c *Client) GetContainerSize(args GetSizeArgs) (*GetSizeValues, error) {
	prms, err := c.client.TestInvoke(
		c.getSizeMethod,
		args.id,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.getSizeMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.getSizeMethod, ln)
	}

	prms, err = client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack items of estimation fields from stack item (%s)", c.getSizeMethod)
	} else if ln := len(prms); ln != 2 {
		return nil, errors.Errorf("unexpected stack item count of estimations fields (%s)", c.getSizeMethod)
	}

	es := Estimations{}

	es.ContainerID, err = client.BytesFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get container ID byte array from stack item (%s)", c.getSizeMethod)
	}

	prms, err = client.ArrayFromStackItem(prms[1])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get estimation list array from stack item (%s)", c.getSizeMethod)
	}

	es.Estimations = make([]Estimation, 0, len(prms))

	for i := range prms {
		arr, err := client.ArrayFromStackItem(prms[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not get estimation struct from stack item (%s)", c.getSizeMethod)
		} else if ln := len(arr); ln != 2 {
			return nil, errors.Errorf("unexpected stack item count of estimation fields (%s)", c.getSizeMethod)
		}

		e := Estimation{}

		e.Reporter, err = client.BytesFromStackItem(arr[0])
		if err != nil {
			return nil, errors.Wrapf(err, "could not get reporter byte array from stack item (%s)", c.getSizeMethod)
		}

		e.Size, err = client.IntFromStackItem(arr[1])
		if err != nil {
			return nil, errors.Wrapf(err, "could not get estimation size from stack item (%s)", c.getSizeMethod)
		}

		es.Estimations = append(es.Estimations, e)
	}

	return &GetSizeValues{
		est: es,
	}, nil
}
