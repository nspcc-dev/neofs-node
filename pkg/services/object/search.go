package object

import (
	"context"
	"errors"
	"math/rand/v2"
	"slices"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	igrpc "github.com/nspcc-dev/neofs-node/internal/grpc"
	inetmap "github.com/nspcc-dev/neofs-node/internal/netmap"
	islices "github.com/nspcc-dev/neofs-node/internal/slices"
	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	protoobject "github.com/nspcc-dev/neofs-sdk-go/proto/object"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/mem"
)

func iterateSearchableContainerNodes(nodeSets [][]netmap.NodeInfo, repRules []uint, ecRules []iec.Rule, allNodes bool, f func(netmap.NodeInfo) bool) {
	for i := range nodeSets {
		var (
			nodeSet = nodeSets[i]
			ecIndex = i - len(repRules)
		)

		if !allNodes && ecIndex >= 0 {
			var (
				partsN    = int(ecRules[ecIndex].ParityPartNum + ecRules[ecIndex].DataPartNum)
				requiredN = int(ecRules[ecIndex].ParityPartNum + 1)
				searchN   = max(requiredN, len(nodeSet)-partsN+requiredN) // CBF 2 and alike.
			)

			if searchN < len(nodeSet) { // Stay safe in case of missing nodes.
				nodeSet = slices.Clone(nodeSet)
				rand.Shuffle(len(nodeSet), func(i, j int) {
					nodeSet[i], nodeSet[j] = nodeSet[j], nodeSet[i]
				})
				nodeSet = nodeSet[:requiredN]
			}
		}
		for _, node := range nodeSet {
			if !f(node) {
				return
			}
		}
	}
}

func (s *Server) forwardSearchRequest(ctx context.Context, req any, nodeSets [][]netmap.NodeInfo) (mem.BufferSlice, error) {
	for _, nodeSet := range nodeSets {
		for _, nodeIdx := range islices.ShuffleIndexes(len(nodeSet)) {
			node, err := s.nodeClients.Get(ctx, nodeSet[nodeIdx])
			if err != nil {
				s.log.Debug("get conn to remote node",
					inetmap.ZapEndpoints(nodeSet[nodeIdx]), zap.Error(err))
				continue
			}

			var respBuf mem.BufferSlice

			err = node.ForAnyGRPCConn(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
				var err error
				respBuf, err = callUnary(ctx, conn, protoobject.ObjectService_SearchV2_FullMethodName, req)
				if err != nil {
					if igrpc.IsUnavailable(err) {
						return clientcore.ErrSkipConnection
					}
					return err
				}

				return nil
			})
			if err == nil {
				return respBuf, nil
			}
			if !errors.Is(err, clientcore.ErrAllConnectionsSkipped) {
				return nil, err
			}

			s.log.Info("remote node is unavailable", zap.String("op", "SEARCH"), inetmap.ZapEndpoints(nodeSet[nodeIdx]))
		}
	}

	return nil, nil
}
