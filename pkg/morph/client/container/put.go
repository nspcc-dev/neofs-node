package container

import (
	"context"
	"fmt"

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

// Put calls Container contract to create container with parameterized
// credentials. If transaction is accepted for processing, Put waits for it to
// be successfully executed. Waiting is performed within ctx,
// [client.ErrTxAwaitTimeout] is returned when it is done.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
func (c *Client) Put(ctx context.Context, p PutPrm) (cid.ID, error) {
	if len(p.sig) == 0 || len(p.key) == 0 {
		return cid.ID{}, errNilArgument
	}

	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.CreateContainerV2Method, []any{
		containerToStackItem(p.cnr), p.sig, p.key, p.token,
	})
	if err == nil {
		return cid.NewFromMarshalledContainer(p.cnr.Marshal()), nil
	}
	if !isMethodNotFoundError(err, fschaincontracts.CreateContainerV2Method) {
		return cid.ID{}, fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.CreateContainerV2Method, err)
	}

	domain := p.cnr.ReadDomain()
	metaAttr := p.cnr.Attribute("__NEOFS__METAINFO_CONSISTENCY")
	metaEnabled := metaAttr == "optimistic" || metaAttr == "strict"
	cnrBytes := p.cnr.Marshal()

	err = c.client.CallWithAlphabetWitness(ctx, fschaincontracts.CreateContainerMethod, []any{
		cnrBytes, p.sig, p.key, p.token, domain.Name(), domain.Zone(), metaEnabled,
	})
	if err != nil {
		if isMethodNotFoundError(err, fschaincontracts.CreateContainerMethod) {
			err = c.client.CallWithAlphabetWitness(ctx, putMethod, []any{
				cnrBytes, p.sig, p.key, p.token, domain.Name(), domain.Zone(), metaEnabled,
			})
			if err != nil {
				return cid.ID{}, fmt.Errorf("could not invoke method (%s): %w", putMethod, err)
			}
			return cid.NewFromMarshalledContainer(cnrBytes), nil
		}
		return cid.ID{}, fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.CreateContainerMethod, err)
	}
	return cid.NewFromMarshalledContainer(cnrBytes), nil
}
