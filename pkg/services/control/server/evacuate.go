package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) EvacuateShard(_ context.Context, req *control.EvacuateShardRequest) (*control.EvacuateShardResponse, error) {
	err := s.isValidRequest(req)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	shardID := shard.NewIDFromBytes(req.GetBody().GetShard_ID())

	var prm engine.EvacuateShardPrm
	prm.WithShardID(shardID)
	prm.WithIgnoreErrors(req.GetBody().GetIgnoreErrors())

	res, err := s.s.Evacuate(prm)
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
