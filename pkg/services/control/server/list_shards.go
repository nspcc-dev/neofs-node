package control

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
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
		si.Blobstor = blobstorInfoToProto(sh.BlobStorInfo)
		si.SetWriteCachePath(sh.WriteCacheInfo.Path)
		si.SetPiloramaPath(sh.PiloramaInfo.Path)

		var m control.ShardMode

		switch sh.Mode {
		case mode.ReadWrite:
			m = control.ShardMode_READ_WRITE
		case mode.ReadOnly:
			m = control.ShardMode_READ_ONLY
		case mode.Degraded:
			m = control.ShardMode_DEGRADED
		case mode.DegradedReadOnly:
			m = control.ShardMode_DEGRADED_READ_ONLY
		default:
			m = control.ShardMode_SHARD_MODE_UNDEFINED
		}

		si.SetMode(m)
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

func blobstorInfoToProto(info blobstor.Info) []*control.BlobstorInfo {
	res := make([]*control.BlobstorInfo, len(info.SubStorages))
	for i := range info.SubStorages {
		res[i] = &control.BlobstorInfo{
			Path: info.SubStorages[i].Path,
			Type: info.SubStorages[i].Type,
		}
	}
	return res
}
