package container

import "github.com/pkg/errors"

// SetEACLArgs groups the arguments
// of set eACL invocation call.
type SetEACLArgs struct {
	eacl []byte // extended ACL table

	sig []byte // eACL table signature
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

// SetEACL invokes the call of set eACL method
// of NeoFS Container contract.
func (c *Client) SetEACL(args SetEACLArgs) error {
	return errors.Wrapf(c.client.Invoke(
		c.setEACLMethod,
		args.eacl,
		args.sig,
	), "could not invoke method (%s)", c.setEACLMethod)
}
