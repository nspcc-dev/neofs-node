package control

import (
	"context"
	"os"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) RestoreShard(_ context.Context, req *control.RestoreShardRequest) (*control.RestoreShardResponse, error) {
	err := s.isValidRequest(req)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// check availability
	err = s.ready()
	if err != nil {
		return nil, err
	}

	shardID := shard.NewIDFromBytes(req.GetBody().GetShard_ID())

	f, err := os.Open(req.GetBody().GetFilepath())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	defer f.Close()

	err = s.storage.RestoreShard(shardID, f, req.GetBody().GetIgnoreErrors())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var resp = &control.RestoreShardResponse{Body: new(control.RestoreShardResponse_Body)}

	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}
