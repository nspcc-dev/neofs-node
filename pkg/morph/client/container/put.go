package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
)

// PutPrm groups parameters of Put operation.
type PutPrm struct {
	cnr   container.Container
	key   []byte
	sig   []byte
	token []byte

	client.InvokePrmOptional
}

// SetContainer sets container.
func (p *PutPrm) SetContainer(cnr container.Container) {
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

// Put saves container with its session token, key and signature
// in NeoFS system through Container contract call.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
func (c *Client) Put(p PutPrm) (cid.ID, error) {
	if len(p.sig) == 0 || len(p.key) == 0 {
		return cid.ID{}, errNilArgument
	}

	var prm client.InvokePrm
	prm.SetMethod(fschaincontracts.CreateContainerMethod)
	prm.InvokePrmOptional = p.InvokePrmOptional

	domain := p.cnr.ReadDomain()
	metaAttr := p.cnr.Attribute("__NEOFS__METAINFO_CONSISTENCY")
	metaEnabled := metaAttr == "optimistic" || metaAttr == "strict"
	cnrBytes := p.cnr.Marshal()
	prm.SetArgs(cnrBytes, p.sig, p.key, p.token, domain.Name(), domain.Zone(), metaEnabled)

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		if isMethodNotFoundError(err, fschaincontracts.CreateContainerMethod) {
			prm.SetMethod(putMethod)
			if err = c.client.Invoke(prm); err != nil {
				return cid.ID{}, fmt.Errorf("could not invoke method (%s): %w", putMethod, err)
			}
			return cid.NewFromMarshalledContainer(cnrBytes), nil
		}
		return cid.ID{}, fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.CreateContainerMethod, err)
	}
	return cid.NewFromMarshalledContainer(cnrBytes), nil
}
