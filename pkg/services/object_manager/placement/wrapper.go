package placement

import (
	"context"

	"github.com/multiformats/go-multiaddr"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	netmapcore "github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/netmap"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
	File source code includes implementations of placement-related solutions.
	Highly specialized interfaces give the opportunity to hide placement implementation in a black box for the reasons:
	  * placement is implementation-tied entity working with graphs, filters, etc.;
	  * NeoFS components are mostly needed in a small part of the solutions provided by placement;
	  * direct dependency from placement avoidance helps other components do not touch crucial changes in placement.
*/

type (
	// CID is a type alias of
	// CID from refs package of neofs-api-go.
	CID = refs.CID

	// SGID is a type alias of
	// SGID from refs package of neofs-api-go.
	SGID = refs.SGID

	// ObjectID is a type alias of
	// ObjectID from refs package of neofs-api-go.
	ObjectID = refs.ObjectID

	// Object is a type alias of
	// Object from object package of neofs-api-go.
	Object = object.Object

	// Address is a type alias of
	// Address from refs package of neofs-api-go.
	Address = refs.Address

	PlacementWrapper struct {
		pl Component
	}
)

var errEmptyPlacement = errors.New("could not create storage lister: empty placement component")

// NewObjectPlacer wraps Component and returns ObjectPlacer interface.
func NewObjectPlacer(pl Component) (*PlacementWrapper, error) {
	if pl == nil {
		return nil, errEmptyPlacement
	}

	return &PlacementWrapper{pl}, nil
}

func (v PlacementWrapper) ContainerNodes(ctx context.Context, cid CID) ([]multiaddr.Multiaddr, error) {
	graph, err := v.pl.Query(ctx, ContainerID(cid))
	if err != nil {
		return nil, errors.Wrap(err, "objectPlacer.ContainerNodes failed on graph query")
	}

	return graph.NodeList()
}

func (v PlacementWrapper) ContainerNodesInfo(ctx context.Context, cid CID, prev int) ([]netmapcore.Info, error) {
	graph, err := v.pl.Query(ctx, ContainerID(cid), UsePreviousNetmap(prev))
	if err != nil {
		return nil, errors.Wrap(err, "objectPlacer.ContainerNodesInfo failed on graph query")
	}

	return graph.NodeInfo()
}

func (v PlacementWrapper) GetNodes(ctx context.Context, addr Address, usePreviousNetMap bool, excl ...multiaddr.Multiaddr) ([]multiaddr.Multiaddr, error) {
	queryOptions := make([]QueryOption, 1, 2)
	queryOptions[0] = ContainerID(addr.CID)

	if usePreviousNetMap {
		queryOptions = append(queryOptions, UsePreviousNetmap(1))
	}

	graph, err := v.pl.Query(ctx, queryOptions...)
	if err != nil {
		if st, ok := status.FromError(errors.Cause(err)); ok && st.Code() == codes.NotFound {
			return nil, container.ErrNotFound
		}

		return nil, errors.Wrap(err, "placer.GetNodes failed on graph query")
	}

	filter := func(group netmap.SFGroup, bucket *netmap.Bucket) *netmap.Bucket {
		return bucket
	}

	if !addr.ObjectID.Empty() {
		filter = func(group netmap.SFGroup, bucket *netmap.Bucket) *netmap.Bucket {
			return bucket.GetSelection(group.Selectors, addr.ObjectID.Bytes())
		}
	}

	return graph.Exclude(excl).Filter(filter).NodeList()
}

func (v PlacementWrapper) IsContainerNode(ctx context.Context, addr multiaddr.Multiaddr, cid CID, previousNetMap bool) (bool, error) {
	nodes, err := v.GetNodes(ctx, Address{
		CID: cid,
	}, previousNetMap)
	if err != nil {
		return false, errors.Wrap(err, "placer.FromContainer failed on placer.GetNodes")
	}

	for i := range nodes {
		if nodes[i].Equal(addr) {
			return true, nil
		}
	}

	return false, nil
}

func (v PlacementWrapper) Epoch() uint64 { return v.pl.NetworkState().Epoch }
