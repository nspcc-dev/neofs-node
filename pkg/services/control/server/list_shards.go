package control

import (
	"context"

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

	for _, shard := range info.Shards {
		si := new(control.ShardInfo)

		si.SetID(*shard.ID)
		si.SetMetabasePath(shard.MetaBaseInfo.Path)
		si.SetBlobstorPath(shard.BlobStorInfo.RootPath)
		si.SetWriteCachePath(shard.WriteCacheInfo.Path)

		// FIXME: use real shard mode when there are more than just `read-write`
		// after https://github.com/nspcc-dev/neofs-node/issues/1044
		si.SetMode(control.ShardMode_READ_WRITE)

		shardInfos = append(shardInfos, si)
	}

	body.SetShards(shardInfos)

	// sign the response
	if err := SignMessage(s.key, resp); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}
