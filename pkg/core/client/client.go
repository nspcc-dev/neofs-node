package clientcore

import (
	"context"
	"errors"
	"io"

	"github.com/nspcc-dev/neofs-sdk-go/client"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	neofscrypto "github.com/nspcc-dev/neofs-sdk-go/crypto"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/quic-go/quic-go"
	"google.golang.org/grpc"
)

// Client is an interface of NeoFS storage
// node's client.
type Client interface {
	ObjectPutInit(ctx context.Context, header object.Object, signer user.Signer, prm client.PrmObjectPutInit) (client.ObjectWriter, error)
	ReplicateObject(ctx context.Context, id oid.ID, src io.ReadSeeker, signer neofscrypto.Signer, signedReplication bool) (*neofscrypto.Signature, error)
	ObjectDelete(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectDelete) (oid.ID, error)
	ObjectGetInit(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectGet) (object.Object, *client.PayloadReader, error)
	ObjectHead(ctx context.Context, containerID cid.ID, objectID oid.ID, signer user.Signer, prm client.PrmObjectHead) (*object.Object, error)
	ObjectSearchInit(ctx context.Context, containerID cid.ID, signer user.Signer, prm client.PrmObjectSearch) (*client.ObjectListReader, error)
	SearchObjects(context.Context, cid.ID, object.SearchFilters, []string, string, neofscrypto.Signer, client.SearchObjectsOptions) ([]client.SearchResultItem, string, error)
	ObjectRangeInit(ctx context.Context, containerID cid.ID, objectID oid.ID, offset, length uint64, signer user.Signer, prm client.PrmObjectRange) (*client.ObjectRangeReader, error)
	AnnounceLocalTrust(ctx context.Context, epoch uint64, trusts []reputation.Trust, prm client.PrmAnnounceLocalTrust) error
	AnnounceIntermediateTrust(ctx context.Context, epoch uint64, trust reputation.PeerToPeerTrust, prm client.PrmAnnounceIntermediateTrust) error
}

// ErrSkipConnection is returned to skip connection.
var ErrSkipConnection = errors.New("connection skipped")

// ErrAllConnectionsSkipped allows to check whether
// [MultiAddressClient.ForAnyGRPCConn] error is returns because all connections
// are unavailable or skipped.
var ErrAllConnectionsSkipped = errors.New("all connections skipped")

// MultiAddressClient is an interface of the
// Client that supports multihost work.
type MultiAddressClient interface {
	Client

	// ForAnyGRPCConn executes op over gRPC connections to given multi-address
	// endpoint-by-endpoint until success.
	//
	// If next endpoint is unavailable or f returns [ErrSkipConnection] for it,
	// ForAnyGRPCConn continues. If this happens on all endpoints, ForAnyGRPCConn
	// returns [ErrAllConnectionsSkipped].
	ForAnyGRPCConn(ctx context.Context, f func(context.Context, *grpc.ClientConn) error) error

	// ForAnyQUICStream executes op over a QUIC connection to the multi-address
	// endpoint-by-endpoint until success.
	ForAnyQUICStream(context.Context, func(context.Context, *quic.Conn, string) error) error
}
