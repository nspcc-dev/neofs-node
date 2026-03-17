package control

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// call only if `ready` returned no error.
func (s *Server) getShardIDList(raw [][]byte) ([]common.ID, error) {
	if len(raw) != 0 {
		res := make([]common.ID, 0, len(raw))
		for i := range raw {
			if len(raw[i]) == 0 {
				return nil, fmt.Errorf("invalid shard ID #%d: empty shard ID", i)
			}
			id, err := common.NewIDFromBytes(raw[i])
			if err != nil {
				return nil, fmt.Errorf("invalid shard ID #%d: %w", i, err)
			}
			res = append(res, id)
		}
		return res, nil
	}

	info := s.storage.DumpInfo()
	res := make([]common.ID, 0, len(info.Shards))
	for i := range info.Shards {
		res = append(res, info.Shards[i].ID)
	}
	return res, nil
}

func (s *Server) ready() error {
	if !s.available.Load() {
		return status.Error(codes.Unavailable, "service has not been completely initialized yet")
	}

	return nil
}
