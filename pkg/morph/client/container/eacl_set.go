package container

import (
	"context"
	"fmt"

	fschaincontracts "github.com/nspcc-dev/neofs-node/pkg/morph/contracts"
)

// PutEACLPrm groups parameters of PutEACL operation.
type PutEACLPrm struct {
	table []byte
	key   []byte
	sig   []byte
	token []byte
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

// PutEACL calls Container contract to set container's extended ACL with
// parameterized credentials. If transaction is accepted for processing, PutEACL
// waits for it to be successfully executed. Waiting is done within ctx,
// [client.ErrTxAwaitTimeout] is returned when it is done.
//
// Returns any error encountered that caused the saving to interrupt.
func (c *Client) PutEACL(ctx context.Context, p PutEACLPrm) error {
	if len(p.sig) == 0 || len(p.key) == 0 {
		return errNilArgument
	}

	err := c.client.CallWithAlphabetWitness(ctx, fschaincontracts.PutContainerEACLMethod, []any{
		p.table, p.sig, p.key, p.token,
	})
	if err != nil {
		if isMethodNotFoundError(err, fschaincontracts.PutContainerEACLMethod) {
			err = c.client.CallWithAlphabetWitness(ctx, setEACLMethod, []any{
				p.table, p.sig, p.key, p.token,
			})
			if err != nil {
				return fmt.Errorf("could not invoke method (%s): %w", setEACLMethod, err)
			}
			return nil
		}
		return fmt.Errorf("could not invoke method (%s): %w", fschaincontracts.PutContainerEACLMethod, err)
	}
	return nil
}
