package container

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
)

// PutPrm groups parameters of Put operation.
type PutPrm struct {
	cnr   container.Container
	key   []byte
	sig   []byte
	token []byte

	eACLTable *eacl.Table
	eaclKey   []byte
	eaclSig   []byte

	origReq []byte
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

// SetEACLTable sets additional table in the same transaction container
// is created.
func (p *PutPrm) SetEACLTable(eaclTable eacl.Table, key, sig []byte) {
	p.eACLTable = &eaclTable
	p.eaclKey = key
	p.eaclSig = sig
}

// SetOriginalRequest sets original API request that triggered contract call.
func (p *PutPrm) SetOriginalRequest(req []byte) {
	p.origReq = req
}

// Put calls Container contract to create container with parameterized
// credentials. If transaction is accepted for processing, Put waits for it to
// be successfully executed. Waiting is performed within ctx,
// [client.ErrTxAwaitTimeout] is returned when it is done.
//
// Returns calculated container identifier and any error
// encountered that caused the saving to interrupt.
func (c *Client) Put(ctx context.Context, p PutPrm) (cid.ID, error) {
	if (len(p.sig) == 0 || len(p.key) == 0) && len(p.origReq) == 0 {
		return cid.ID{}, errNilArgument
	}

	if p.eACLTable != nil {
		return c.putContainerWithEacl(ctx, p)
	}

	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.CreateContainerV2Method, putCnrArgs(p))
	if err != nil {
		if errors.Is(err, client.ErrTxAwaitTimeout) {
			// it sent TX correctly but its status is unknown,
			// container ID may be helpful, it is the only
			// outcome left after Put call
			return cid.NewFromMarshalledContainer(p.cnr.Marshal()), fmt.Errorf("(%s) tx invocation timeout: %w", fschaincontracts.CreateContainerV2Method, err)
		}

		return cid.ID{}, fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.CreateContainerV2Method, err)
	}

	return cid.NewFromMarshalledContainer(p.cnr.Marshal()), nil
}

func (c *Client) putContainerWithEacl(ctx context.Context, p PutPrm) (cid.ID, error) {
	var (
		addr     = c.ContractAddress()
		b        = smartcontract.NewBuilder()
		rawTable = p.eACLTable.Marshal()
	)

	b.InvokeMethod(addr, fschaincontracts.CreateContainerV2Method, putCnrArgs(p)...)
	b.InvokeMethod(addr, fschaincontracts.PutContainerEACLMethod, rawTable, p.eaclSig, p.eaclKey, p.token)
	script, err := b.Script()
	if err != nil {
		panic(fmt.Errorf("cannon build complex script for two contract calls: [%s, %s], error: %w", fschaincontracts.CreateContainerV2Method, fschaincontracts.PutContainerEACLMethod, err))
	}

	err = c.client.RunScriptForAlphabet(ctx, script)
	if err != nil {
		return cid.ID{}, fmt.Errorf("notary request invocation: %w", err)
	}

	return cid.NewFromMarshalledContainer(p.cnr.Marshal()), nil
}

func putCnrArgs(p PutPrm) []any {
	var (
		args  []any
		cnrSI = containerToStackItem(p.cnr)
	)
	if p.origReq == nil {
		args = []any{cnrSI, p.sig, p.key, p.token}
	} else {
		args = []any{cnrSI, p.origReq}
	}
	return args
}
