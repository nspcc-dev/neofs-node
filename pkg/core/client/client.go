package client

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	rawclient "github.com/nspcc-dev/neofs-api-go/rpc/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
)

// Client is an interface of NeoFS storage
// node's client.
type Client interface {
	client.Client

	// RawForAddress must return rawclient.Client
	// for the passed network.Address.
	RawForAddress(network.Address) *rawclient.Client
}

// NodeInfo groups information about NeoFS storage node needed for Client construction.
type NodeInfo struct {
	addrGroup network.AddressGroup

	key []byte
}

// SetAddressGroup sets group of network addresses.
func (x *NodeInfo) SetAddressGroup(v network.AddressGroup) {
	x.addrGroup = v
}

// AddressGroup returns group of network addresses.
func (x NodeInfo) AddressGroup() network.AddressGroup {
	return x.addrGroup
}

// SetPublicKey sets public key in a binary format.
//
// Argument must not be mutated.
func (x *NodeInfo) SetPublicKey(v []byte) {
	x.key = v
}

// PublicKey returns public key in a binary format.
//
// Result must not be mutated.
func (x *NodeInfo) PublicKey() []byte {
	return x.key
}
