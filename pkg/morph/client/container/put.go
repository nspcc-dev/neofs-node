package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// PutArgs groups the arguments
// of put container invocation call.
type PutArgs struct {
	cnr []byte // container in a binary format

	sig []byte // binary container signature

	publicKey []byte // public key of container owner

	token []byte // binary session token

	name, zone string // native name and zone

	client.InvokePrmOptional
}

// SetPublicKey sets the public key of container owner
// in a binary format.
func (p *PutArgs) SetPublicKey(v []byte) {
	p.publicKey = v
}

// SetContainer sets the container structure
// in a binary format.
func (p *PutArgs) SetContainer(v []byte) {
	p.cnr = v
}

// SetSignature sets the container structure
// owner's signature.
func (p *PutArgs) SetSignature(v []byte) {
	p.sig = v
}

// SetSessionToken sets token of the session
// within which the container was created
// in a binary format.
func (p *PutArgs) SetSessionToken(v []byte) {
	p.token = v
}

// SetNativeNameWithZone sets container native name and its zone.
func (p *PutArgs) SetNativeNameWithZone(name, zone string) {
	p.name, p.zone = name, zone
}

// Put invokes the call of put (named if name is set) container method
// of NeoFS Container contract.
func (c *Client) Put(args PutArgs) error {
	var (
		method string
		prm    client.InvokePrm
	)

	if args.name != "" {
		method = c.putNamedMethod

		prm.SetArgs(args.cnr, args.sig, args.publicKey, args.token, args.name, args.zone)
	} else {
		method = c.putMethod

		prm.SetArgs(args.cnr, args.sig, args.publicKey, args.token)
	}

	prm.SetMethod(method)
	prm.InvokePrmOptional = args.InvokePrmOptional

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", method, err)
	}

	return nil
}
