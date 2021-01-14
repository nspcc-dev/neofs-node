package container

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

// EACLArgs groups the arguments
// of get eACL test invoke call.
type EACLArgs struct {
	cid []byte // container identifier
}

// EACLValues groups the stack parameters
// returned by get eACL test invoke.
type EACLValues struct {
	eacl []byte // extended ACL table

	signature []byte // RFC-6979 signature of extended ACL table

	publicKey []byte // public key of the extended ACL table signer
}

// SetCID sets the container identifier
// in a binary format.
func (g *EACLArgs) SetCID(v []byte) {
	g.cid = v
}

// EACL returns the eACL table
// in a binary format.
func (g *EACLValues) EACL() []byte {
	return g.eacl
}

// Signature returns RFC-6979 signature of extended ACL table.
func (g *EACLValues) Signature() []byte {
	return g.signature
}

// PublicKey of the signature.
func (g *EACLValues) PublicKey() []byte {
	return g.publicKey
}

// EACL performs the test invoke of get eACL
// method of NeoFS Container contract.
func (c *Client) EACL(args EACLArgs) (*EACLValues, error) {
	prms, err := c.client.TestInvoke(
		c.eaclMethod,
		args.cid,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.eaclMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.eaclMethod, ln)
	}

	arr, err := client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get item array of eACL (%s)", c.eaclMethod)
	}

	if len(arr) != 3 {
		return nil, errors.Errorf("unexpected eacl stack item count (%s): %d", c.eaclMethod, len(arr))
	}

	eacl, err := client.BytesFromStackItem(arr[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get byte array of eACL (%s)", c.eaclMethod)
	}

	sig, err := client.BytesFromStackItem(arr[1])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get byte array of eACL signature (%s)", c.eaclMethod)
	}

	pub, err := client.BytesFromStackItem(arr[2])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get byte array of eACL public key (%s)", c.eaclMethod)
	}

	return &EACLValues{
		eacl:      eacl,
		signature: sig,
		publicKey: pub,
	}, nil
}
