package tree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	cidSDK "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapSDK "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

type movePair struct {
	cid    cidSDK.ID
	treeID string
	op     *pilorama.LogMove
}

type replicationTask struct {
	n   netmapSDK.NodeInfo
	req *ApplyRequest
}

type applyOp struct {
	treeID string
	pilorama.CIDDescriptor
	pilorama.Move
}

const (
	defaultReplicatorCapacity    = 64
	defaultReplicatorWorkerCount = 64
	defaultReplicatorSendTimeout = time.Second * 5
)

func (s *Service) localReplicationWorker() {
	for {
		select {
		case <-s.closeCh:
			return
		case op := <-s.replicateLocalCh:
			err := s.forest.TreeApply(op.CIDDescriptor, op.treeID, &op.Move, false)
			if err != nil {
				s.log.Error("failed to apply replicated operation",
					zap.String("err", err.Error()))
			}
		}
	}
}

func (s *Service) replicationWorker() {
	for {
		select {
		case <-s.closeCh:
			return
		case task := <-s.replicationTasks:
			var lastErr error
			var lastAddr string

			task.n.IterateNetworkEndpoints(func(addr string) bool {
				lastAddr = addr

				c, err := s.cache.get(context.Background(), addr)
				if err != nil {
					lastErr = fmt.Errorf("can't create client: %w", err)
					return false
				}

				ctx, cancel := context.WithTimeout(context.Background(), s.replicatorTimeout)
				_, lastErr = c.Apply(ctx, task.req)
				cancel()

				return lastErr == nil
			})

			if lastErr != nil {
				if errors.Is(lastErr, errRecentlyFailed) {
					s.log.Debug("do not send update to the node",
						zap.String("last_error", lastErr.Error()))
				} else {
					s.log.Warn("failed to sent update to the node",
						zap.String("last_error", lastErr.Error()),
						zap.String("address", lastAddr),
						zap.String("key", hex.EncodeToString(task.n.PublicKey())))
				}
			}
		}
	}
}

func (s *Service) replicateLoop(ctx context.Context) {
	for i := 0; i < s.replicatorWorkerCount; i++ {
		go s.replicationWorker()
		go s.localReplicationWorker()
	}
	defer func() {
		for len(s.replicationTasks) != 0 {
			<-s.replicationTasks
		}
	}()

	for {
		select {
		case <-s.closeCh:
			return
		case <-ctx.Done():
			return
		case op := <-s.replicateCh:
			err := s.replicate(op)
			if err != nil {
				s.log.Error("error during replication",
					zap.String("err", err.Error()),
					zap.Stringer("cid", op.cid),
					zap.String("treeID", op.treeID))
			}
		}
	}
}

func (s *Service) replicate(op movePair) error {
	req := newApplyRequest(&op)
	err := SignMessage(req, s.key)
	if err != nil {
		return fmt.Errorf("can't sign data: %w", err)
	}

	nodes, localIndex, err := s.getContainerNodes(op.cid)
	if err != nil {
		return fmt.Errorf("can't get container nodes: %w", err)
	}

	for i := range nodes {
		if i != localIndex {
			s.replicationTasks <- replicationTask{nodes[i], req}
		}
	}
	return nil
}

func (s *Service) pushToQueue(cid cidSDK.ID, treeID string, op *pilorama.LogMove) {
	select {
	case s.replicateCh <- movePair{
		cid:    cid,
		treeID: treeID,
		op:     op,
	}:
	default:
	}
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
