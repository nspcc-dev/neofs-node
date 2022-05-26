package tree

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"time"

	clientcore "github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

type movePair struct {
	cid    *cidSDK.ID
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
	err := signMessage(req, s.key)
	if err != nil {
		return fmt.Errorf("can't sign data: %w", err)
	}

	nodes, err := s.getContainerNodes(op.cid)
	if err != nil {
		return fmt.Errorf("can't get container nodes: %w", err)
	}

	var node clientcore.NodeInfo
	for _, n := range nodes.Flatten() {
		var lastErr error

		if bytes.Equal(n.NodeInfo.PublicKey(), s.rawPub) {
			continue
		}

		n.NodeInfo.IterateAddresses(func(addr string) bool {
			c, err := dialTreeService(ctx, addr)
			if err != nil {
				lastErr = err
				return false
			}

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

func (s *Service) pushToQueue(cid *cidSDK.ID, treeID string, op *pilorama.LogMove) {
	s.replicateCh <- movePair{
		cid:    cid,
		treeID: treeID,
		op:     op,
	}
}

func (s *Service) getContainerNodes(cid *cidSDK.ID) (netmapSDK.ContainerNodes, error) {
	nm, err := s.nmSource.GetNetMap(0)
	if err != nil {
		return nil, fmt.Errorf("can't get netmap: %w", err)
	}

	cnr, err := s.cnrSource.Get(cid)
	if err != nil {
		return nil, fmt.Errorf("can't get container: %w", err)
	}

	policy := cnr.PlacementPolicy()
	return nm.GetContainerNodes(policy, cid.ToV2().GetValue())
}

func newApplyRequest(op *movePair) *ApplyRequest {
	return &ApplyRequest{
		Body: &ApplyRequest_Body{
			ContainerId: op.cid.ToV2().GetValue(),
			TreeId:      op.treeID,
			Operation: &LogMove{
				ParentId: op.op.Parent,
				Meta:     op.op.Meta.Bytes(),
				ChildId:  op.op.Child,
			},
		},
	}
}
