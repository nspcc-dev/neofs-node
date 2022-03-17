package control

import (
	"context"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) SetShardMode(_ context.Context, req *control.SetShardModeRequest) (*control.SetShardModeResponse, error) {
	// verify request
	err := s.isValidRequest(req)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	var (
		mode shard.Mode

		requestedMode  = req.GetBody().GetMode()
		requestedShard = shard.NewIDFromBytes(req.Body.GetShard_ID())
	)

	switch requestedMode {
	case control.ShardMode_READ_WRITE:
		mode = shard.ModeReadWrite
	case control.ShardMode_READ_ONLY:
		mode = shard.ModeReadOnly
	case control.ShardMode_DEGRADED:
		mode = shard.ModeDegraded
	default:
		return nil, status.Error(codes.Internal, fmt.Sprintf("unknown shard mode: %s", requestedMode))
	}

	err = s.s.SetShardMode(requestedShard, mode, false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// create and fill response
	resp := new(control.SetShardModeResponse)

	body := new(control.SetShardModeResponse_Body)
	resp.SetBody(body)

	// sign the response
	err = SignMessage(s.key, resp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
