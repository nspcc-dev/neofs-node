package client

import (
	"context"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
)

// Client is an interface of NeoFS storage
// node's client.
type Client interface {
	ContainerAnnounceUsedSpace(context.Context, client.PrmAnnounceSpace) (*client.ResAnnounceSpace, error)
	ObjectPutInit(context.Context, client.PrmObjectPutInit) (*client.ObjectWriter, error)
	ObjectDelete(context.Context, client.PrmObjectDelete) (*client.ResObjectDelete, error)
	ObjectGetInit(context.Context, client.PrmObjectGet) (*client.ObjectReader, error)
	ObjectHead(context.Context, client.PrmObjectHead) (*client.ResObjectHead, error)
	ObjectSearchInit(context.Context, client.PrmObjectSearch) (*client.ObjectListReader, error)
	ObjectRangeInit(context.Context, client.PrmObjectRange) (*client.ObjectRangeReader, error)
	ObjectHash(context.Context, client.PrmObjectHash) (*client.ResObjectHash, error)
	AnnounceLocalTrust(context.Context, client.PrmAnnounceLocalTrust) (*client.ResAnnounceLocalTrust, error)
	AnnounceIntermediateTrust(context.Context, client.PrmAnnounceIntermediateTrust) (*client.ResAnnounceIntermediateTrust, error)
	ExecRaw(f func(client *rawclient.Client) error) error
	Close() error
}

// MultiAddressClient is an interface of the
// Client that supports multihost work.
type MultiAddressClient interface {
	Client

	// RawForAddress must return rawclient.Client
	// for the passed network.Address.
	RawForAddress(network.Address, func(cli *rawclient.Client) error) error

	ReportError(error)
}

// NodeInfo groups information about a NeoFS storage node needed for Client construction.
type NodeInfo struct {
	addrGroup network.AddressGroup

	externalAddrGroup network.AddressGroup

	key []byte
}

// SetAddressGroup sets a group of network addresses.
func (x *NodeInfo) SetAddressGroup(v network.AddressGroup) {
	x.addrGroup = v
}

// AddressGroup returns a group of network addresses.
func (x NodeInfo) AddressGroup() network.AddressGroup {
	return x.addrGroup
}

// SetExternalAddressGroup sets an external group of network addresses.
func (x *NodeInfo) SetExternalAddressGroup(v network.AddressGroup) {
	x.externalAddrGroup = v
}

// ExternalAddressGroup returns a group of network addresses.
func (x NodeInfo) ExternalAddressGroup() network.AddressGroup {
	return x.externalAddrGroup
}

// SetPublicKey sets a public key in a binary format.
//
// Argument must not be mutated.
func (x *NodeInfo) SetPublicKey(v []byte) {
	x.key = v
}

// PublicKey returns a public key in a binary format.
//
// Result must not be mutated.
func (x *NodeInfo) PublicKey() []byte {
	return x.key
}
