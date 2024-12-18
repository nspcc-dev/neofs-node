package client

import (
	"context"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	reputationSDK "github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"google.golang.org/grpc"
)

// Client is an interface of NeoFS storage
// node's client.
type Client interface {
	ContainerAnnounceUsedSpace(ctx context.Context, announcements []container.SizeEstimation, prm client.PrmAnnounceSpace) error
	ObjectPutInit(ctx context.Context, header object.Object, signer user.Signer, prm client.PrmObjectPutInit) (client.ObjectWriter, error)
	ReplicateObject(ctx context.Context, id oid.ID, src io.ReadSeeker, signer neofscrypto.Signer) error
	ObjectDelete(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectDelete) (oid.ID, error)
	ObjectGetInit(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectGet) (object.Object, *client.PayloadReader, error)
	ObjectHead(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectHead) (*object.Object, error)
	ObjectSearchInit(ctx context.Context, containerID cid.ID, signer user.Signer, prm client.PrmObjectSearch) (*client.ObjectListReader, error)
	ObjectRangeInit(ctx context.Context, containerID cid.ID, objectID oid.ID, offset, length uint64, signer user.Signer, prm client.PrmObjectRange) (*client.ObjectRangeReader, error)
	ObjectHash(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectHash) ([][]byte, error)
	AnnounceLocalTrust(ctx context.Context, epoch uint64, trusts []reputationSDK.Trust, prm client.PrmAnnounceLocalTrust) error
	AnnounceIntermediateTrust(ctx context.Context, epoch uint64, trust reputationSDK.PeerToPeerTrust, prm client.PrmAnnounceIntermediateTrust) error
	ExecRaw(f func(*grpc.ClientConn) error) error
	Close() error
}

// MultiAddressClient is an interface of the
// Client that supports multihost work.
type MultiAddressClient interface {
	Client

	// RawForAddress executes op over gRPC connections to given multi-address
	// endpoint-by-endpoint until success.
	RawForAddress(multiAddr network.Address, op func(*grpc.ClientConn) error) error

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
