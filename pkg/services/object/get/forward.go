package getsvc

import (
	"context"
	"errors"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	inetmap "github.com/nspcc-dev/neofs-node/internal/netmap"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

// ForwardRequestFunc forwards original request to remote node through passed
// connection and responds to the client.
//
// If response stream fails, [ErrResponseStreamFailure] is returned.
//
// If node is currently unavailable, [ErrUnavailableNode] is returned.
type ForwardRequestFunc func(ctx context.Context, node clientcore.MultiAddressClient) error

func (s *Service) forwardRequest(ctx context.Context, repRules []uint, ecRules []iec.Rule, sortedNodeLists [][]netmap.NodeInfo, op string, forwardRequestFn ForwardRequestFunc) error {
	// try to hit primary REP holders first
	for i := range repRules {
		if repRules[i] <= 1 {
			if fin, err := s.forwardRequestToNode(ctx, sortedNodeLists[i][0], op, forwardRequestFn); fin {
				return err
			}
			continue
		}

		// randomize load on equilibrium nodes
		for _, nodeIdx := range islices.ShuffleIndexes(int(repRules[i])) {
			if fin, err := s.forwardRequestToNode(ctx, sortedNodeLists[i][nodeIdx], op, forwardRequestFn); fin {
				return err
			}
		}
	}

	// try reserve REP holders
	for i := range repRules {
		// shuffling does not make sense anymore
		for nodeIdx := repRules[i]; nodeIdx <= uint(len(sortedNodeLists[i])); nodeIdx++ {
			if fin, err := s.forwardRequestToNode(ctx, sortedNodeLists[i][nodeIdx], op, forwardRequestFn); fin {
				return err
			}
		}
	}

	// same for EC
	sortedNodeLists = sortedNodeLists[len(repRules):]

	for i := range ecRules {
		for _, nodeIdx := range islices.ShuffleIndexes(int(ecRules[i].DataPartNum + ecRules[i].ParityPartNum)) {
			if fin, err := s.forwardRequestToNode(ctx, sortedNodeLists[i][nodeIdx], op, forwardRequestFn); fin {
				return err
			}
		}
	}

	for i := range ecRules {
		for nodeIdx := int(ecRules[i].DataPartNum + ecRules[i].ParityPartNum); nodeIdx <= len(sortedNodeLists[i]); nodeIdx++ {
			if fin, err := s.forwardRequestToNode(ctx, sortedNodeLists[i][nodeIdx], op, forwardRequestFn); fin {
				return err
			}
		}
	}

	return apistatus.ErrObjectNotFound
}

func (s *Service) forwardRequestToNode(ctx context.Context, node netmap.NodeInfo, op string, forwardRequestFn ForwardRequestFunc) (bool, error) {
	conn, err := s.conns.(*clientCacheWrapper).connect(ctx, node)
	if err != nil {
		s.log.Debug("get conn to remote node",
			inetmap.ZapEndpoints(node), zap.Error(err))
		return false, nil
	}

	err = forwardRequestFn(ctx, conn)
	if !errors.Is(err, ErrUnavailableNode) {
		return true, err
	}

	s.log.Info("remote node is unavailable", zap.String("op", op), inetmap.ZapEndpoints(node))

	return false, nil
}
