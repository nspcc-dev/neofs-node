package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/services/control"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) ListShards(_ context.Context, req *control.ListShardsRequest) (*control.ListShardsResponse, error) {
	// verify request
	if err := s.isValidRequest(req); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// create and fill response
	resp := new(control.ListShardsResponse)

	body := new(control.ListShardsResponse_Body)
	resp.SetBody(body)

	info := s.s.DumpInfo()

	shardInfos := make([]*control.ShardInfo, 0, len(info.Shards))

	for _, sh := range info.Shards {
		si := new(control.ShardInfo)

		si.SetID(*sh.ID)
		si.SetMetabasePath(sh.MetaBaseInfo.Path)
		si.SetBlobstorPath(sh.BlobStorInfo.RootPath)
		si.SetWriteCachePath(sh.WriteCacheInfo.Path)
		si.SetPiloramaPath(sh.PiloramaInfo.Path)

		var mode control.ShardMode

		switch sh.Mode {
		case shard.ModeReadWrite:
			mode = control.ShardMode_READ_WRITE
		case shard.ModeReadOnly:
			mode = control.ShardMode_READ_ONLY
		case shard.ModeDegraded:
			mode = control.ShardMode_DEGRADED
		default:
			mode = control.ShardMode_SHARD_MODE_UNDEFINED
		}

		si.SetMode(mode)
		si.SetErrorCount(sh.ErrorCount)

		shardInfos = append(shardInfos, si)
	}

	body.SetShards(shardInfos)

	// sign the response
	if err := SignMessage(s.key, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
