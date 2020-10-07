package wrapper

import (
	"crypto/sha256"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	v2container "github.com/nspcc-dev/neofs-api-go/v2/container"
	msgContainer "github.com/nspcc-dev/neofs-api-go/v2/container/grpc"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/pkg/errors"
)

var (
	errNilArgument = errors.New("empty argument")
	errUnsupported = errors.New("unsupported structure version")
)

// Put saves passed container structure in NeoFS system
// through Container contract call.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
func (w *Wrapper) Put(cnr *container.Container, pubKey, signature []byte) (*container.ID, error) {
	if cnr == nil || len(pubKey) == 0 || len(signature) == 0 {
		return nil, errNilArgument
	}

	args := client.PutArgs{}
	args.SetPublicKey(pubKey)
	args.SetSignature(signature)

	id := new(container.ID)

	if v2 := cnr.ToV2(); v2 == nil {
		return nil, errUnsupported // use other major version if there any
	} else {
		data, err := v2.StableMarshal(nil)
		if err != nil {
			return nil, errors.Wrap(err, "can't marshal container")
		}

		id.SetSHA256(sha256.Sum256(data))
		args.SetContainer(data)
	}

	return id, w.client.Put(args)
}

// Get reads the container from NeoFS system by identifier
// through Container contract call.
//
// If an empty slice is returned for the requested identifier,
// storage.ErrNotFound error is returned.
func (w *Wrapper) Get(cid *container.ID) (*container.Container, error) {
	if cid == nil {
		return nil, errNilArgument
	}

	args := client.GetArgs{}

	if v2 := cid.ToV2(); v2 == nil {
		return nil, errUnsupported // use other major version if there any
	} else {
		args.SetCID(v2.GetValue())
	}

	// ask RPC neo node to get serialized container
	rpcAnswer, err := w.client.Get(args)
	if err != nil {
		return nil, err
	}

	// In #37 we've decided to remove length check, because smart contract would
	// fail on casting `nil` value from storage to `[]byte` producing FAULT state.
	// Apparently it does not fail, so we have to check length explicitly.
	if len(rpcAnswer.Container()) == 0 {
		return nil, core.ErrNotFound
	}

	// convert serialized bytes into GRPC structure
	grpcMsg := new(msgContainer.Container)
	err = grpcMsg.Unmarshal(rpcAnswer.Container())
	if err != nil {
		// use other major version if there any
		return nil, errors.Wrap(err, "can't unmarshal container")
	}

	// convert GRPC structure into SDK structure, used in the code
	v2Cnr := v2container.ContainerFromGRPCMessage(grpcMsg)

	return container.NewContainerFromV2(v2Cnr), nil
}

// Delete removes the container from NeoFS system
// through Container contract call.
//
// Returns any error encountered that caused
// the removal to interrupt.
func (w *Wrapper) Delete(cid *container.ID, signature []byte) error {
	if cid == nil || len(signature) == 0 {
		return errNilArgument
	}

	args := client.DeleteArgs{}
	args.SetSignature(signature)

	if v2 := cid.ToV2(); v2 == nil {
		return errUnsupported // use other major version if there any
	} else {
		args.SetCID(v2.GetValue())
	}

	return w.client.Delete(args)
}

// List returns a list of container identifiers belonging
// to the specified owner of NeoFS system. The list is composed
// through Container contract call.
//
// Returns the identifiers of all NeoFS containers if pointer
// to owner identifier is nil.
func (w *Wrapper) List(ownerID *owner.ID) ([]*container.ID, error) {
	args := client.ListArgs{}

	if ownerID == nil {
		args.SetOwnerID([]byte{})
	} else if v2 := ownerID.ToV2(); v2 == nil {
		return nil, errUnsupported // use other major version if there any
	} else {
		args.SetOwnerID(v2.GetValue())
	}

	// ask RPC neo node to get serialized container
	rpcAnswer, err := w.client.List(args)
	if err != nil {
		return nil, err
	}

	rawIDs := rpcAnswer.CIDList()
	result := make([]*container.ID, 0, len(rawIDs))

	for i := range rawIDs {
		v2 := new(v2refs.ContainerID)
		v2.SetValue(rawIDs[i])

		cid := container.NewIDFromV2(v2)

		result = append(result, cid)
	}

	return result, nil
}
