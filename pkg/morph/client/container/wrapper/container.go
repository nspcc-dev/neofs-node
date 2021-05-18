package wrapper

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
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

	id := container.NewID()

	data, err := cnr.Marshal()
	if err != nil {
		return nil, fmt.Errorf("can't marshal container: %w", err)
	}

	id.SetSHA256(sha256.Sum256(data))
	args.SetContainer(data)

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

	v2 := cid.ToV2()
	if v2 == nil {
		return nil, errUnsupported // use other major version if there any
	}

	args.SetCID(v2.GetValue())

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

	// unmarshal container
	cnr := container.New()
	if err := cnr.Unmarshal(rpcAnswer.Container()); err != nil {
		// use other major version if there any
		return nil, fmt.Errorf("can't unmarshal container: %w", err)
	}

	return cnr, nil
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

	v2 := cid.ToV2()
	if v2 == nil {
		return errUnsupported // use other major version if there any
	}

	args.SetCID(v2.GetValue())

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

// AnnounceLoad saves container size estimation calculated by storage node
// with key in NeoFS system through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (w *Wrapper) AnnounceLoad(a container.UsedSpaceAnnouncement, key []byte) error {
	v2 := a.ContainerID().ToV2()
	if v2 == nil {
		return errUnsupported // use other major version if there any
	}

	args := client.PutSizeArgs{}
	args.SetContainerID(v2.GetValue())
	args.SetEpoch(a.Epoch())
	args.SetSize(a.UsedSpace())
	args.SetReporterKey(key)

	return w.client.PutSize(args)
}

// EstimationID is an identity of container load estimation inside Container contract.
type EstimationID []byte

// ListLoadEstimationsByEpoch returns a list of container load estimations for to the specified epoch.
// The list is composed through Container contract call.
func (w *Wrapper) ListLoadEstimationsByEpoch(epoch uint64) ([]EstimationID, error) {
	args := client.ListSizesArgs{}
	args.SetEpoch(epoch)

	// ask RPC neo node to get serialized container
	rpcAnswer, err := w.client.ListSizes(args)
	if err != nil {
		return nil, err
	}

	rawIDs := rpcAnswer.IDList()
	result := make([]EstimationID, 0, len(rawIDs))

	for i := range rawIDs {
		result = append(result, rawIDs[i])
	}

	return result, nil
}

// Estimation is a structure of single container load estimation
// reported by storage node.
type Estimation struct {
	Size uint64

	Reporter []byte
}

// Estimation is a structure of grouped container load estimation inside Container contract.
type Estimations struct {
	ContainerID *container.ID

	Values []Estimation
}

// GetUsedSpaceEstimations returns a list of container load estimations by ID.
// The list is composed through Container contract call.
func (w *Wrapper) GetUsedSpaceEstimations(id EstimationID) (*Estimations, error) {
	args := client.GetSizeArgs{}
	args.SetID(id)

	rpcAnswer, err := w.client.GetContainerSize(args)
	if err != nil {
		return nil, err
	}

	es := rpcAnswer.Estimations()

	v2 := new(v2refs.ContainerID)
	v2.SetValue(es.ContainerID)

	res := &Estimations{
		ContainerID: container.NewIDFromV2(v2),
		Values:      make([]Estimation, 0, len(es.Estimations)),
	}

	for i := range es.Estimations {
		res.Values = append(res.Values, Estimation{
			Size:     uint64(es.Estimations[i].Size),
			Reporter: es.Estimations[i].Reporter,
		})
	}

	return res, nil
}
