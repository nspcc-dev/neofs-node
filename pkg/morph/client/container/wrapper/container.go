package wrapper

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	v2refs "github.com/nspcc-dev/neofs-api-go/v2/refs"
	core "github.com/nspcc-dev/neofs-node/pkg/core/container"
	staticli "github.com/nspcc-dev/neofs-node/pkg/morph/client"
	client "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
	"github.com/nspcc-dev/neofs-sdk-go/session"
	"github.com/nspcc-dev/neofs-sdk-go/signature"
)

var (
	errNilArgument = errors.New("empty argument")
	errUnsupported = errors.New("unsupported structure version")
)

// Put marshals container, and passes it to Wrapper's Put method
// along with sig.Key() and sig.Sign().
//
// Returns error if container is nil.
func Put(w *Wrapper, cnr *container.Container) (*cid.ID, error) {
	if cnr == nil {
		return nil, errNilArgument
	}

	data, err := cnr.Marshal()
	if err != nil {
		return nil, fmt.Errorf("can't marshal container: %w", err)
	}

	binToken, err := cnr.SessionToken().Marshal()
	if err != nil {
		return nil, fmt.Errorf("could not marshal session token: %w", err)
	}

	sig := cnr.Signature()

	name, zone := container.GetNativeNameWithZone(cnr)

	err = w.Put(PutPrm{
		cnr:   data,
		key:   sig.Key(),
		sig:   sig.Sign(),
		token: binToken,
		name:  name,
		zone:  zone,
	})
	if err != nil {
		return nil, err
	}

	id := cid.New()
	id.SetSHA256(sha256.Sum256(data))

	return id, nil
}

// PutPrm groups parameters of Put operation.
type PutPrm struct {
	cnr   []byte
	key   []byte
	sig   []byte
	token []byte
	name  string
	zone  string

	staticli.InvokePrmOptional
}

// SetContainer sets container data.
func (p *PutPrm) SetContainer(cnr []byte) {
	p.cnr = cnr
}

// SetKey sets public key.
func (p *PutPrm) SetKey(key []byte) {
	p.key = key
}

// SetSignature sets signature.
func (p *PutPrm) SetSignature(sig []byte) {
	p.sig = sig
}

// SetToken sets session token.
func (p *PutPrm) SetToken(token []byte) {
	p.token = token
}

// SetName sets native name.
func (p *PutPrm) SetName(name string) {
	p.name = name
}

// SetZone sets zone.
func (p *PutPrm) SetZone(zone string) {
	p.zone = zone
}

// Put saves binary container with its session token, key and signature
// in NeoFS system through Container contract call.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
//
// If TryNotary is provided, calls notary contract.
func (w *Wrapper) Put(prm PutPrm) error {
	if len(prm.sig) == 0 || len(prm.key) == 0 {
		return errNilArgument
	}

	var args client.PutArgs

	args.SetContainer(prm.cnr)
	args.SetSignature(prm.sig)
	args.SetPublicKey(prm.key)
	args.SetSessionToken(prm.token)
	args.SetNativeNameWithZone(prm.name, prm.zone)
	args.InvokePrmOptional = prm.InvokePrmOptional

	err := w.client.Put(args)
	if err != nil {
		return err
	}

	return nil
}

type containerSource Wrapper

func (x *containerSource) Get(cid *cid.ID) (*container.Container, error) {
	return Get((*Wrapper)(x), cid)
}

// AsContainerSource provides container Source interface
// from Wrapper instance.
func AsContainerSource(w *Wrapper) core.Source {
	return (*containerSource)(w)
}

// Get marshals container ID, and passes it to Wrapper's Get method.
//
// Returns error if cid is nil.
func Get(w *Wrapper, cid *cid.ID) (*container.Container, error) {
	return w.Get(cid.ToV2().GetValue())
}

// Get reads the container from NeoFS system by binary identifier
// through Container contract call.
//
// If an empty slice is returned for the requested identifier,
// storage.ErrNotFound error is returned.
func (w *Wrapper) Get(cid []byte) (*container.Container, error) {
	var args client.GetArgs

	args.SetCID(cid)

	// ask RPC neo node to get serialized container
	rpcAnswer, err := w.client.Get(args)
	if err != nil {
		// TODO(fyrchik): reuse messages from container contract.
		// Currently there are some dependency problems:
		// github.com/nspcc-dev/neofs-node/pkg/innerring imports
		//        github.com/nspcc-dev/neofs-sdk-go/audit imports
		//        github.com/nspcc-dev/neofs-api-go/v2/audit: ambiguous import: found package github.com/nspcc-dev/neofs-api-go/v2/audit in multiple modules:
		//        github.com/nspcc-dev/neofs-api-go v1.27.1 (/home/dzeta/go/pkg/mod/github.com/nspcc-dev/neofs-api-go@v1.27.1/v2/audit)
		//        github.com/nspcc-dev/neofs-api-go/v2 v2.11.0-pre.0.20211201134523-3604d96f3fe1 (/home/dzeta/go/pkg/mod/github.com/nspcc-dev/neofs-api-go/v2@v2.11.0-pre.0.20211201134523-3604d96f3fe1/audit)
		if strings.Contains(err.Error(), "container does not exist") {
			return nil, core.ErrNotFound
		}
		return nil, err
	}

	// unmarshal container
	cnr := container.New()
	if err := cnr.Unmarshal(rpcAnswer.Container()); err != nil {
		// use other major version if there any
		return nil, fmt.Errorf("can't unmarshal container: %w", err)
	}

	binToken := rpcAnswer.SessionToken()
	if len(binToken) > 0 {
		tok := session.NewToken()

		err = tok.Unmarshal(binToken)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal session token: %w", err)
		}

		cnr.SetSessionToken(tok)
	}

	sig := signature.New()
	sig.SetKey(rpcAnswer.PublicKey())
	sig.SetSign(rpcAnswer.Signature())

	cnr.SetSignature(sig)

	return cnr, nil
}

// Delete marshals container ID, and passes it to Wrapper's Delete method
// along with signature and session token.
//
// Returns error if container ID is nil.
func Delete(w *Wrapper, witness core.RemovalWitness) error {
	id := witness.ContainerID()
	if id == nil {
		return errNilArgument
	}

	binToken, err := witness.SessionToken().Marshal()
	if err != nil {
		return fmt.Errorf("could not marshal session token: %w", err)
	}

	return w.Delete(
		DeletePrm{
			cid:       id.ToV2().GetValue(),
			signature: witness.Signature(),
			token:     binToken,
		})
}

// DeletePrm groups parameters of Delete client operation.
type DeletePrm struct {
	cid       []byte
	signature []byte
	token     []byte

	staticli.InvokePrmOptional
}

// SetCID sets container ID.
func (d *DeletePrm) SetCID(cid []byte) {
	d.cid = cid
}

// SetSignature sets signature.
func (d *DeletePrm) SetSignature(signature []byte) {
	d.signature = signature
}

// SetToken sets session token.
func (d *DeletePrm) SetToken(token []byte) {
	d.token = token
}

// Delete removes the container from NeoFS system
// through Container contract call.
//
// Returns any error encountered that caused
// the removal to interrupt.
//
// If TryNotary is provided, calls notary contract.
func (w *Wrapper) Delete(prm DeletePrm) error {
	if len(prm.signature) == 0 {
		return errNilArgument
	}

	var args client.DeleteArgs

	args.SetSignature(prm.signature)
	args.SetCID(prm.cid)
	args.SetSessionToken(prm.token)
	args.InvokePrmOptional = prm.InvokePrmOptional

	return w.client.Delete(args)
}

// List returns a list of container identifiers belonging
// to the specified owner of NeoFS system. The list is composed
// through Container contract call.
//
// Returns the identifiers of all NeoFS containers if pointer
// to owner identifier is nil.
func (w *Wrapper) List(ownerID *owner.ID) ([]*cid.ID, error) {
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
	result := make([]*cid.ID, 0, len(rawIDs))

	for i := range rawIDs {
		v2 := new(v2refs.ContainerID)
		v2.SetValue(rawIDs[i])

		id := cid.NewFromV2(v2)

		result = append(result, id)
	}

	return result, nil
}

// AnnounceLoadPrm groups parameters of AnnounceLoad operation.
type AnnounceLoadPrm struct {
	a   container.UsedSpaceAnnouncement
	key []byte

	staticli.InvokePrmOptional
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
func (w *Wrapper) AnnounceLoad(prm AnnounceLoadPrm) error {
	v2 := prm.a.ContainerID().ToV2()
	if v2 == nil {
		return errUnsupported // use other major version if there any
	}

	args := client.PutSizeArgs{}
	args.SetContainerID(v2.GetValue())
	args.SetEpoch(prm.a.Epoch())
	args.SetSize(prm.a.UsedSpace())
	args.SetReporterKey(prm.key)
	args.InvokePrmOptional = prm.InvokePrmOptional

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

// Estimations is a structure of grouped container load estimation inside Container contract.
type Estimations struct {
	ContainerID *cid.ID

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
		ContainerID: cid.NewFromV2(v2),
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
