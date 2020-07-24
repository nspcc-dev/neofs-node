package netmap

import (
	"github.com/pkg/errors"
)

// PeerInfo groups the parameters of
// new NeoFS peer.
type PeerInfo struct {
	address []byte // peer network address in a binary format

	key []byte // peer public key

	opts [][]byte // binary peer options
}

// AddPeerArgs groups the arguments
// of add peer invocation call.
type AddPeerArgs struct {
	info PeerInfo // peer information
}

const addPeerFixedArgNumber = 2

// Address returns the peer network address
// in a binary format.
//
// Address format is dictated by application
// architecture.
func (a PeerInfo) Address() []byte {
	return a.address
}

// SetAddress sets the peer network address
// in a binary format.
//
// Address format is dictated by application
// architecture.
func (a *PeerInfo) SetAddress(v []byte) {
	a.address = v
}

// PublicKey returns the peer public key
// in a binary format.
//
// Key format is dictated by application
// architecture.
func (a PeerInfo) PublicKey() []byte {
	return a.key
}

// SetPublicKey sets the peer public key
// in a binary format.
//
// Key format is dictated by application
// architecture.
func (a *PeerInfo) SetPublicKey(v []byte) {
	a.key = v
}

// Options returns the peer options
// in a binary format.
//
// Option format is dictated by application
// architecture.
func (a PeerInfo) Options() [][]byte {
	return a.opts
}

// SetOptions sets the peer options
// in a binary format.
//
// Option format is dictated by application
// architecture.
func (a *PeerInfo) SetOptions(v [][]byte) {
	a.opts = v
}

// SetInfo sets the peer information.
func (a *AddPeerArgs) SetInfo(v PeerInfo) {
	a.info = v
}

// AddPeer invokes the call of add peer method
// of NeoFS Netmap contract.
func (c *Client) AddPeer(args AddPeerArgs) error {
	info := args.info

	invokeArgs := make([]interface{}, 0, addPeerFixedArgNumber+len(info.opts))

	invokeArgs = append(invokeArgs,
		info.address,
		info.key,
	)

	for i := range info.opts {
		invokeArgs = append(invokeArgs, info.opts[i])
	}

	return errors.Wrapf(c.client.Invoke(
		c.addPeerMethod,
		invokeArgs...,
	), "could not invoke method (%s)", c.addPeerMethod)
}
