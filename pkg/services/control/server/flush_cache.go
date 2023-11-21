package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
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

	for _, shardID := range s.getShardIDList(req.GetBody().GetShard_ID()) {
		var prm engine.FlushWriteCachePrm
		prm.SetShardID(shardID)

		_, err = s.storage.FlushWriteCache(prm)
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
