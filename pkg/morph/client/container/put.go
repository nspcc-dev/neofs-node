package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-api-go/v2/refs"
	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// Put marshals container, and passes it to Wrapper's Put method
// along with sig.Key() and sig.Sign().
//
// Returns error if container is nil.
func Put(c *Client, cnr containercore.Container) (*cid.ID, error) {
	data := cnr.Value.Marshal()

	d := container.ReadDomain(cnr.Value)

	var prm PutPrm
	prm.SetContainer(data)
	prm.SetName(d.Name())
	prm.SetZone(d.Zone())

	if cnr.Session != nil {
		prm.SetToken(cnr.Session.Marshal())
	}

	// TODO(@cthulhu-rider): #1387 implement and use another approach to avoid conversion
	var sigV2 refs.Signature
	cnr.Signature.WriteToV2(&sigV2)

	prm.SetKey(sigV2.GetKey())
	prm.SetSignature(sigV2.GetSign())

	err := c.Put(prm)
	if err != nil {
		return nil, err
	}

	var id cid.ID
	container.CalculateIDFromBinary(&id, data)

	return &id, nil
}

// PutPrm groups parameters of Put operation.
type PutPrm struct {
	cnr   []byte
	key   []byte
	sig   []byte
	token []byte
	name  string
	zone  string

	client.InvokePrmOptional
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
func (c *Client) Put(p PutPrm) error {
	if len(p.sig) == 0 || len(p.key) == 0 {
		return errNilArgument
	}

	var (
		method string
		prm    client.InvokePrm
	)

	if p.name != "" {
		method = putNamedMethod
		prm.SetArgs(p.cnr, p.sig, p.key, p.token, p.name, p.zone)
	} else {
		method = putMethod
		prm.SetArgs(p.cnr, p.sig, p.key, p.token)
	}

	prm.SetMethod(method)
	prm.InvokePrmOptional = p.InvokePrmOptional

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", method, err)
	}
	return nil
}
