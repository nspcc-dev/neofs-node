package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) EvacuateShard(_ context.Context, req *control.EvacuateShardRequest) (*control.EvacuateShardResponse, error) {
	err := s.isValidRequest(req)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// check availability
	err = s.ready()
	if err != nil {
		return nil, err
	}

	count, err := s.storage.Evacuate(s.getShardIDList(req.GetBody().GetShard_ID()), req.GetBody().GetIgnoreErrors(), s.replicate)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &control.EvacuateShardResponse{
		Body: &control.EvacuateShardResponse_Body{
			Count: uint32(count),
		},
	}

	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) replicate(addr oid.Address, obj *objectSDK.Object) error {
	cid := obj.GetContainerID()
	if cid.IsZero() {
		// Return nil to prevent situations where a shard can't be evacuated
		// because of a single bad/corrupted object.
		return nil
	}

	pi, err := iec.GetPartInfo(*obj)
	if err != nil {
		s.log.Error("invalid EC part object detected", zap.Stringer("object", addr), zap.Error(err))
		return nil // skip object
	}

	nm, err := s.netMapSrc.NetMap()
	if err != nil {
		return err
	}

	c, err := s.cnrSrc.Get(cid)
	if err != nil {
		return err
	}

	policy := c.PlacementPolicy()
	ecRules := policy.ECRules()
	var totalECParts int
	if pi.RuleIndex >= 0 {
		if pi.RuleIndex >= len(ecRules) { // covers non-EC container
			s.log.Error("rule index overflows total number of EC rules in part object", zap.Stringer("object", addr),
				zap.Int("ruleIdx", pi.RuleIndex), zap.Int("totalRules", len(ecRules)))
			return nil // skip object
		}
		totalECParts = int(ecRules[pi.RuleIndex].DataPartNum() + ecRules[pi.RuleIndex].ParityPartNum())
		if pi.Index >= totalECParts {
			s.log.Error("part index overflows total number of EC parts in part object", zap.Stringer("object", addr),
				zap.Int("partIdx", pi.Index), zap.Int("totalParts", totalECParts))
			return nil // skip object
		}
	}

	ns, err := nm.ContainerNodes(policy, cid)
	if err != nil {
		return fmt.Errorf("can't build a list of container nodes: %w", err)
	}

	var nodes []netmap.NodeInfo
	if pi.RuleIndex >= 0 {
		n := ns[pi.RuleIndex]
		for i := range iec.NodeSequenceForPart(pi.Index, totalECParts, len(n)) {
			if s.nodeState.IsLocalNodePublicKey(n[i].PublicKey()) {
				break
			}
			nodes = append(nodes, n[i])
		}
	} else {
		nodes = slices.Concat(ns...)
		bs := (*keys.PublicKey)(&s.key.PublicKey).Bytes()
		nodes = slices.DeleteFunc(nodes, func(info netmap.NodeInfo) bool {
			return bytes.Equal(info.PublicKey(), bs)
		})
	}

	var res replicatorResult
	var task replicator.Task
	task.SetObject(obj)
	task.SetObjectAddress(addr)
	task.SetCopiesNumber(1)
	task.SetNodes(nodes)
	s.replicator.HandleTask(context.TODO(), task, &res)

	if res.count == 0 {
		return errors.New("object was not replicated")
	}
	return nil
}

type replicatorResult struct {
	count int
}

// SubmitSuccessfulReplication implements the replicator.TaskResult interface.
func (r *replicatorResult) SubmitSuccessfulReplication(_ netmap.NodeInfo) {
	r.count++
}
