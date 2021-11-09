package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// SetEACLArgs groups the arguments
// of set eACL invocation call.
type SetEACLArgs struct {
	eacl []byte // extended ACL table

	sig []byte // eACL table signature

	pubkey []byte // binary public key

	token []byte // binary session token
}

// SetEACL sets the extended ACL table
// in a binary format.
func (p *SetEACLArgs) SetEACL(v []byte) {
	p.eacl = v
}

// SetSignature sets the eACL table structure
// owner's signature.
func (p *SetEACLArgs) SetSignature(v []byte) {
	p.sig = v
}

// SetPublicKey sets public key related to
// table signature.
func (p *SetEACLArgs) SetPublicKey(v []byte) {
	p.pubkey = v
}

// SetSessionToken sets token of the session
// within which the eACL table was set
// in a binary format.
func (p *SetEACLArgs) SetSessionToken(v []byte) {
	p.token = v
}

// SetEACL invokes the call of set eACL method
// of NeoFS Container contract.
func (c *Client) SetEACL(args SetEACLArgs) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.setEACLMethod)
	prm.SetArgs(args.eacl, args.sig, args.pubkey, args.token)

	err := c.client.Invoke(prm)

	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.setEACLMethod, err)
	}
	return nil
}
