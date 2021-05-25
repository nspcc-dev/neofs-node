package container

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// GetArgs groups the arguments
// of get container test invoke call.
type GetArgs struct {
	cid []byte // container identifier
}

// GetValues groups the stack parameters
// returned by get container test invoke.
type GetValues struct {
	cnr []byte // container in a binary form

	signature []byte // RFC-6979 signature of container

	publicKey []byte // public key of the container signer

	token []byte // token of the session within which the container was created
}

// SetCID sets the container identifier
// in a binary format.
func (g *GetArgs) SetCID(v []byte) {
	g.cid = v
}

// Container returns the container
// in a binary format.
func (g *GetValues) Container() []byte {
	return g.cnr
}

// Signature returns RFC-6979 signature of the container.
func (g *GetValues) Signature() []byte {
	return g.signature
}

// PublicKey returns public key related to signature.
func (g *GetValues) PublicKey() []byte {
	return g.publicKey
}

// SessionToken returns token of the session within which
// the container was created in a NeoFS API binary format.
func (g *GetValues) SessionToken() []byte {
	return g.token
}

// Get performs the test invoke of get container
// method of NeoFS Container contract.
func (c *Client) Get(args GetArgs) (*GetValues, error) {
	prms, err := c.client.TestInvoke(
		c.getMethod,
		args.cid,
	)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", c.getMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", c.getMethod, ln)
	}

	arr, err := client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get item array of container (%s): %w", c.getMethod, err)
	}

	if len(arr) != 4 {
		return nil, fmt.Errorf("unexpected container stack item count (%s): %d", c.getMethod, len(arr))
	}

	cnrBytes, err := client.BytesFromStackItem(arr[0])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of container (%s): %w", c.getMethod, err)
	}

	sig, err := client.BytesFromStackItem(arr[1])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of container signature (%s): %w", c.getMethod, err)
	}

	pub, err := client.BytesFromStackItem(arr[2])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of public key (%s): %w", c.getMethod, err)
	}

	tok, err := client.BytesFromStackItem(arr[3])
	if err != nil {
		return nil, fmt.Errorf("could not get byte array of session token (%s): %w", c.getMethod, err)
	}

	return &GetValues{
		cnr:       cnrBytes,
		signature: sig,
		publicKey: pub,
		token:     tok,
	}, nil
}
