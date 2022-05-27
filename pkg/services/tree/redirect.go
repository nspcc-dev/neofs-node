package tree

import (
	"context"
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var errNoSuitableNode = errors.New("no node was found to execute the request")

// forEachNode executes callback for each node in the container until true is returned.
// Returns errNoSuitableNode if there was no successful attempt to dial any node.
func (s *Service) forEachNode(ctx context.Context, nodes netmap.Nodes, f func(c TreeServiceClient) bool) error {
	var called bool
	for _, node := range nodes {
		var stop bool
		node.IterateAddresses(func(endpoint string) bool {
			c, err := dialTreeService(ctx, endpoint)
			if err != nil {
				return false
			}

			s.log.Debug("redirecting tree service query", zap.String("endpoint", endpoint))
			called = true
			stop = f(c)
			return true
		})
		if stop {
			return nil
		}
	}
	if !called {
		return errNoSuitableNode
	}
	return nil
}

func dialTreeService(ctx context.Context, netmapAddr string) (TreeServiceClient, error) {
	var netAddr network.Address
	if err := netAddr.FromString(netmapAddr); err != nil {
		return nil, err
	}

	cc, err := grpc.DialContext(ctx, netAddr.URIAddr(), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return NewTreeServiceClient(cc), nil
}
