package client

import (
	"context"
	"io"

	rawclient "github.com/nspcc-dev/neofs-api-go/v2/rpc/client"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	"github.com/nspcc-dev/neofs-sdk-go/container"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/eacl"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
)

// Client is an interface of NeoFS storage
// node's client.
type Client interface {
	GetBalance(context.Context, *owner.ID, ...client.CallOption) (*client.BalanceOfRes, error)

	PutContainer(context.Context, *container.Container, ...client.CallOption) (*client.ContainerPutRes, error)
	GetContainer(context.Context, *cid.ID, ...client.CallOption) (*client.ContainerGetRes, error)
	ListContainers(context.Context, *owner.ID, ...client.CallOption) (*client.ContainerListRes, error)
	DeleteContainer(context.Context, *cid.ID, ...client.CallOption) (*client.ContainerDeleteRes, error)

	EACL(context.Context, *cid.ID, ...client.CallOption) (*client.EACLRes, error)
	SetEACL(context.Context, *eacl.Table, ...client.CallOption) (*client.SetEACLRes, error)

	AnnounceContainerUsedSpace(context.Context, []container.UsedSpaceAnnouncement, ...client.CallOption) (*client.AnnounceSpaceRes, error)

	EndpointInfo(context.Context, ...client.CallOption) (*client.EndpointInfoRes, error)
	NetworkInfo(context.Context, ...client.CallOption) (*client.NetworkInfoRes, error)

	PutObject(context.Context, *client.PutObjectParams, ...client.CallOption) (*client.ObjectPutRes, error)
	DeleteObject(context.Context, *client.DeleteObjectParams, ...client.CallOption) (*client.ObjectDeleteRes, error)
	GetObject(context.Context, *client.GetObjectParams, ...client.CallOption) (*client.ObjectGetRes, error)
	HeadObject(context.Context, *client.ObjectHeaderParams, ...client.CallOption) (*client.ObjectHeadRes, error)
	SearchObjects(context.Context, *client.SearchObjectParams, ...client.CallOption) (*client.ObjectSearchRes, error)
	ObjectPayloadRangeData(context.Context, *client.RangeDataParams, ...client.CallOption) (*client.ObjectRangeRes, error)
	HashObjectPayloadRanges(context.Context, *client.RangeChecksumParams, ...client.CallOption) (*client.ObjectRangeHashRes, error)

	AnnounceLocalTrust(context.Context, client.AnnounceLocalTrustPrm, ...client.CallOption) (*client.AnnounceLocalTrustRes, error)
	AnnounceIntermediateTrust(context.Context, client.AnnounceIntermediateTrustPrm, ...client.CallOption) (*client.AnnounceIntermediateTrustRes, error)

	CreateSession(context.Context, uint64, ...client.CallOption) (*client.CreateSessionRes, error)

	Raw() *rawclient.Client

	Conn() io.Closer
}

// MultiAddressClient is an interface of the
// Client that supports multihost work.
type MultiAddressClient interface {
	Client

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
