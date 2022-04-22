package tree

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/api/transport/grpc"
)

type movePair struct {
	cid    cidSDK.ID
	treeID string
	op     *pilorama.LogMove
}

const (
	defaultReplicatorCapacity = 64
	defaultReplicatorTimeout  = time.Second * 2
)

func (s *Service) replicateLoop(ctx context.Context) {
	for {
		select {
		case <-s.closeCh:
		case <-ctx.Done():
			return
		case op := <-s.replicateCh:
			ctx, cancel := context.WithTimeout(ctx, defaultReplicatorTimeout)
			err := s.replicate(ctx, op)
			cancel()

			if err != nil {
				s.log.Error("error during replication",
					zap.String("err", err.Error()),
					zap.Stringer("cid", op.cid),
					zap.String("treeID", op.treeID))
			}
		}
	}
}

func (s *Service) replicate(ctx context.Context, op movePair) error {
	req := newApplyRequest(&op)
	// TODO(@fyrchik): #1328 access control
	//err := signature.SignDataWithHandler(s.key, req, func(key, sign []byte) {
	//	req.Signature = &Signature{
	//		Key:  key,
	//		Sign: sign,
	//	}
	//})
	//if err != nil {
	//	return fmt.Errorf("can't sign data: %w", err)
	//}

	nodes, err := s.getContainerNodes(op.cid)
	if err != nil {
		return fmt.Errorf("can't get container nodes: %w", err)
	}

	var node clientcore.NodeInfo
	for _, n := range nodes {
		var lastErr error

		n.IterateNetworkEndpoints(func(addr string) bool {
			cc, err := grpc.Dial(ctx, option.WithEndpoint(addr))
			if err != nil {
				lastErr = err
				return false
			}

			// TODO cache clients
			c := NewTreeServiceClient(cc)

			_, lastErr = c.Apply(ctx, req)
			return lastErr == nil
		})

		if lastErr != nil {
			s.log.Warn("failed to sent update to the node",
				zap.String("last_error", lastErr.Error()),
				zap.String("address", network.StringifyGroup(node.AddressGroup())),
				zap.String("key", base64.StdEncoding.EncodeToString(node.PublicKey())))
		}
	}
	return nil
}

func (s *Service) pushToQueue(cid cidSDK.ID, treeID string, op *pilorama.LogMove) {
	s.replicateCh <- movePair{
		cid:    cid,
		treeID: treeID,
		op:     op,
	}
}

func (s *Service) getContainerNodes(cid cidSDK.ID) ([]netmapSDK.NodeInfo, error) {
	nm, err := s.nmSource.GetNetMap(0)
	if err != nil {
		return nil, fmt.Errorf("can't get netmap: %w", err)
	}

	cnr, err := s.cnrSource.Get(cid)
	if err != nil {
		return nil, fmt.Errorf("can't get container: %w", err)
	}

	policy := cnr.Value.PlacementPolicy()
	rawCID := make([]byte, sha256.Size)
	cid.Encode(rawCID)

	nodes, err := nm.ContainerNodes(policy, rawCID)
	if err != nil {
		return nil, err
	}

	return placement.FlattenNodes(nodes), nil
}

func newApplyRequest(op *movePair) *ApplyRequest {
	rawCID := make([]byte, sha256.Size)
	op.cid.Encode(rawCID)

	return &ApplyRequest{
		Body: &ApplyRequest_Body{
			ContainerId: rawCID,
			TreeId:      op.treeID,
			Operation: &LogMove{
				ParentId: op.op.Parent,
				Meta:     op.op.Meta.Bytes(),
				ChildId:  op.op.Child,
			},
		},
	}
}
