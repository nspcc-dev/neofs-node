package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/services/replicator"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

	var prm engine.EvacuateShardPrm
	prm.WithShardIDList(s.getShardIDList(req.GetBody().GetShard_ID()))
	prm.WithIgnoreErrors(req.GetBody().GetIgnoreErrors())
	prm.WithFaultHandler(s.replicate)

	res, err := s.storage.Evacuate(prm)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &control.EvacuateShardResponse{
		Body: &control.EvacuateShardResponse_Body{
			Count: uint32(res.Count()),
		},
	}

	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}

func (s *Server) replicate(addr oid.Address, obj *objectSDK.Object) error {
	cid, ok := obj.ContainerID()
	if !ok {
		// Return nil to prevent situations where a shard can't be evacuated
		// because of a single bad/corrupted object.
		return nil
	}

	nm, err := s.netMapSrc.GetNetMap(0)
	if err != nil {
		return err
	}

	c, err := s.cnrSrc.Get(cid)
	if err != nil {
		return err
	}

	ns, err := nm.ContainerNodes(c.Value.PlacementPolicy(), cid)
	if err != nil {
		return fmt.Errorf("can't build a list of container nodes")
	}

	nodes := placement.FlattenNodes(ns)
	bs := (*keys.PublicKey)(&s.key.PublicKey).Bytes()
	for i := 0; i < len(nodes); i++ {
		if bytes.Equal(nodes[i].PublicKey(), bs) {
			copy(nodes[i:], nodes[i+1:])
			nodes = nodes[:len(nodes)-1]
		}
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
