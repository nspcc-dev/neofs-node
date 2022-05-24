package tree

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var errNoSuitableNode = errors.New("no node was found to execute the request")

// forEachNode executes callback for each node in the container.
// If the node belongs to a container, nil error is returned.
// Otherwise, f is executed for each node, stopping if true is returned.
func (s *Service) forEachNode(ctx context.Context, cid cidSDK.ID, f func(c TreeServiceClient) bool) error {
	cntNodes, err := s.getContainerNodes(cid)
	if err != nil {
		return fmt.Errorf("can't get container nodes for %s: %w", cid, err)
	}

	rawPub := (*keys.PublicKey)(&s.key.PublicKey).Bytes()
	for _, n := range cntNodes {
		if bytes.Equal(n.PublicKey(), rawPub) {
			return nil
		}
	}

	var called bool
	for _, n := range cntNodes {
		var stop bool
		n.IterateNetworkEndpoints(func(endpoint string) bool {
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
