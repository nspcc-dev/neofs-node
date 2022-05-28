package tree

import (
	"bytes"
	"context"
	"errors"

	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

var errNoSuitableNode = errors.New("no node was found to execute the request")

// forEachNode executes callback for each node in the container until true is returned.
// Returns errNoSuitableNode if there was no successful attempt to dial any node.
func (s *Service) forEachNode(ctx context.Context, cntNodes []netmapSDK.NodeInfo, f func(c TreeServiceClient) bool) error {
	for _, n := range cntNodes {
		if bytes.Equal(n.PublicKey(), s.rawPub) {
			return nil
		}
	}

	var called bool
	for _, n := range cntNodes {
		var stop bool
		n.IterateNetworkEndpoints(func(endpoint string) bool {
			c, err := s.cache.get(ctx, endpoint)
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
