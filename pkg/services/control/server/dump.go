package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) DumpShard(_ context.Context, req *control.DumpShardRequest) (*control.DumpShardResponse, error) {
	err := s.isValidRequest(req)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	shardID := shard.NewIDFromBytes(req.GetBody().GetShard_ID())

	var prm shard.DumpPrm
	prm.WithPath(req.GetBody().GetFilepath())
	prm.WithIgnoreErrors(req.GetBody().GetIgnoreErrors())

	err = s.s.DumpShard(shardID, prm)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := new(control.DumpShardResponse)
	resp.SetBody(new(control.DumpShardResponse_Body))

	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}
