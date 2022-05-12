package container

import (
	"crypto/sha256"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// Put marshals container, and passes it to Wrapper's Put method
// along with sig.Key() and sig.Sign().
//
// Returns error if container is nil.
func Put(c *Client, cnr *container.Container) (*cid.ID, error) {
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

	err = c.Put(PutPrm{
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

	var id cid.ID
	id.SetSHA256(sha256.Sum256(data))

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
