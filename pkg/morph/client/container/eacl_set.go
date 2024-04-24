package container

import (
	"fmt"

	containercore "github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// PutEACL marshals table, and passes it to Wrapper's PutEACLBinary method
// along with sig.Key() and sig.Sign().
//
// Returns error if table is nil.
func PutEACL(c *Client, eaclInfo containercore.EACL) error {
	if eaclInfo.Value == nil {
		return errNilArgument
	}

	data, err := eaclInfo.Value.Marshal()
	if err != nil {
		return fmt.Errorf("can't marshal eacl table: %w", err)
	}

	var prm PutEACLPrm
	prm.SetTable(data)

	if eaclInfo.Session != nil {
		prm.SetToken(eaclInfo.Session.Marshal())
	}

	prm.SetKey(eaclInfo.Signature.PublicKeyBytes())
	prm.SetSignature(eaclInfo.Signature.Value())
	prm.RequireAlphabetSignature()

	return c.PutEACL(prm)
}

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
