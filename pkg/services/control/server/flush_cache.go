package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) FlushCache(_ context.Context, req *control.FlushCacheRequest) (*control.FlushCacheResponse, error) {
	err := s.isValidRequest(req)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// check availability
	err = s.ready()
	if err != nil {
		return nil, err
	}

	shardIDs, err := s.getShardIDList(req.GetBody().GetShard_ID())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	for _, shardID := range shardIDs {
		err = s.storage.FlushWriteCache(shardID)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	resp := &control.FlushCacheResponse{Body: &control.FlushCacheResponse_Body{}}

	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return resp, nil
}
