package wrapper

import (
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/pkg/errors"
)

// OwnerID represents the container owner identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/storage.OwnerID.
type OwnerID = storage.OwnerID

// Container represents the NeoFS Container structure.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/storage.Container.
type Container = storage.Container

// Put saves passed container structure in NeoFS system
// through Container contract call.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
func (w *Wrapper) Put(cnr *Container) (*CID, error) {
	// calculate container identifier
	//
	// Note: cid is used as return value only, but the calculation is performed
	// primarily in order to catch potential error before contract client call.
	cid, err := container.CalculateID(cnr)
	if err != nil {
		return nil, errors.Wrap(err, "could not calculate container identifier")
	}

	// marshal the container
	cnrBytes, err := cnr.MarshalBinary()
	if err != nil {
		return nil, errors.Wrap(err, "could not marshal the container")
	}

	// prepare invocation arguments
	args := contract.PutArgs{}
	args.SetOwnerID(cnr.OwnerID().Bytes())
	args.SetContainer(cnrBytes)
	args.SetSignature(nil) // TODO: set signature from request when will appear.

	// invoke smart contract call
	if err := w.client.Put(args); err != nil {
		return nil, errors.Wrap(err, "could not invoke smart contract")
	}

	return cid, nil
}

// Get reads the container from NeoFS system by identifier
// through Container contract call.
//
// If an empty slice is returned for the requested identifier,
// storage.ErrNotFound error is returned.
func (w *Wrapper) Get(cid CID) (*Container, error) {
	// prepare invocation arguments
	args := contract.GetArgs{}
	args.SetCID(cid.Bytes())

	// invoke smart contract call
	values, err := w.client.Get(args)
	if err != nil {
		return nil, errors.Wrap(err, "could not invoke smart contract")
	}

	cnrBytes := values.Container()
	if len(cnrBytes) == 0 {
		return nil, storage.ErrNotFound
	}

	cnr := new(Container)

	// unmarshal the container
	if err := cnr.UnmarshalBinary(cnrBytes); err != nil {
		return nil, errors.Wrap(err, "could not unmarshal container")
	}

	return cnr, nil
}

// Delete removes the container from NeoFS system
// through Container contract call.
//
// Returns any error encountered that caused
// the removal to interrupt.
func (w *Wrapper) Delete(cid CID) error {
	// prepare invocation arguments
	args := contract.DeleteArgs{}
	args.SetCID(cid.Bytes())
	args.SetOwnerID(nil)   // TODO: add owner ID when will appear.
	args.SetSignature(nil) // TODO: add CID signature when will appear.

	// invoke smart contract call
	//
	// Note: errors.Wrap return nil on nil error arg.
	return errors.Wrap(
		w.client.Delete(args),
		"could not invoke smart contract",
	)
}

// List returns a list of container identifiers belonging
// to the specified owner of NeoFS system. The list is composed
// through Container contract call.
//
// Returns the identifiers of all NeoFS containers if pointer
// to owner identifier is nil.
func (w *Wrapper) List(ownerID *OwnerID) ([]CID, error) {
	// prepare invocation arguments
	args := contract.ListArgs{}

	// Note: by default owner identifier slice is nil,
	// so client won't attach invocation arguments.
	// This behavior matches the nil argument of current method.
	// If argument is not nil, we must specify owner identifier.
	if ownerID != nil {
		args.SetOwnerID(ownerID.Bytes())
	}

	// invoke smart contract call
	values, err := w.client.List(args)
	if err != nil {
		return nil, errors.Wrap(err, "could not invoke smart contract")
	}

	binCIDList := values.CIDList()
	cidList := make([]CID, 0, len(binCIDList))

	// unmarshal all container identifiers
	for i := range binCIDList {
		cid, err := refs.CIDFromBytes(binCIDList[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not decode container ID #%d", i)
		}

		cidList = append(cidList, cid)
	}

	return cidList, nil
}
