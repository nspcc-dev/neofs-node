package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// PutEACLPrm groups parameters of PutEACL operation.
type PutEACLPrm struct {
	table []byte
	key   []byte
	sig   []byte
	token []byte

	client.InvokePrmOptional
}

// SetTable sets table.
func (p *PutEACLPrm) SetTable(table []byte) {
	p.table = table
}

// SetKey sets key.
func (p *PutEACLPrm) SetKey(key []byte) {
	p.key = key
}

// SetSignature sets signature.
func (p *PutEACLPrm) SetSignature(sig []byte) {
	p.sig = sig
}

// SetToken sets session token.
func (p *PutEACLPrm) SetToken(token []byte) {
	p.token = token
}

// PutEACL saves binary eACL table with its session token, key and signature
// in NeoFS system through Container contract call.
//
// Returns any error encountered that caused the saving to interrupt.
func (c *Client) PutEACL(p PutEACLPrm) error {
	if len(p.sig) == 0 || len(p.key) == 0 {
		return errNilArgument
	}

	prm := client.InvokePrm{}
	prm.SetMethod(setEACLMethod)
	prm.SetArgs(p.table, p.sig, p.key, p.token)
	prm.InvokePrmOptional = p.InvokePrmOptional

	// no magic bugs with notary requests anymore, this operation should
	// _always_ be notary signed so make it one more time even if it is
	// a repeated flag setting
	prm.RequireAlphabetSignature()

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", setEACLMethod, err)
	}
	return nil
}
