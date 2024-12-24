package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// PutPrm groups parameters of Put operation.
type PutPrm struct {
	cnr               []byte
	key               []byte
	sig               []byte
	token             []byte
	name              string
	zone              string
	enableMetaOnChain bool

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

// EnableMeta enables meta-on-chain.
func (p *PutPrm) EnableMeta() {
	p.enableMetaOnChain = true
}

// Put saves binary container with its session token, key and signature
// in NeoFS system through Container contract call.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
func (c *Client) Put(p PutPrm) error {
	if len(p.sig) == 0 || len(p.key) == 0 {
		return errNilArgument
	}

	var prm client.InvokePrm
	prm.SetMethod(putMethod)
	prm.InvokePrmOptional = p.InvokePrmOptional
	prm.SetArgs(p.cnr, p.sig, p.key, p.token, p.name, p.zone, p.enableMetaOnChain)

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", putMethod, err)
	}
	return nil
}
